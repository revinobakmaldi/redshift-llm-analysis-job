#!/usr/bin/env python3
import json
import logging
import os
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import redshift_connector
import requests
from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("redshift-llm-analysis-job")


@dataclass
class Config:
    redshift_host: str
    redshift_port: int
    redshift_database: str
    redshift_user: str
    redshift_password: str
    redshift_schema: str
    source_sql_path: Path
    target_table: str
    llm_base_url: str
    llm_model: str
    llm_api_key: str
    llm_timeout_seconds: int
    llm_max_tokens: int
    llm_temperature: float
    llm_system_prompt_path: Path
    job_name: str
    source_name: str
    source_preview_rows: int


DEFAULT_SYSTEM_PROMPT = """You are an operations analyst for a QSR business. Analyze the provided dataset and return valid JSON only.

The main business metrics are:
- sales
- transaction
- basket size
- speed of service
- transaction per manhour
- upsell rate

Output JSON schema:
{
  \"headline\": string,
  \"summary\": string,
  \"key_findings\": [string],
  \"risks\": [string],
  \"opportunities\": [string],
  \"recommended_actions\": [string],
  \"confidence\": number,
  \"metrics_observed\": [{\"name\": string, \"value\": string, \"comment\": string}]
}

Rules:
- Return JSON only, no markdown.
- Base conclusions only on the supplied data.
- Prioritize operational interpretation for store performance, throughput, labor productivity, service speed, and commercial execution.
- Explicitly reason about tradeoffs between sales growth, staffing efficiency, service speed, and upsell behavior.
- If data is limited, say so in summary or risks.
- Keep recommended_actions specific, practical, and suitable for QSR operators or area managers.
"""


def require(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def load_config() -> Config:
    return Config(
        redshift_host=require("REDSHIFT_HOST"),
        redshift_port=int(os.getenv("REDSHIFT_PORT", "5439")),
        redshift_database=require("REDSHIFT_DATABASE"),
        redshift_user=require("REDSHIFT_USER"),
        redshift_password=require("REDSHIFT_PASSWORD"),
        redshift_schema=os.getenv("REDSHIFT_SCHEMA", "public"),
        source_sql_path=BASE_DIR / os.getenv("SOURCE_SQL_PATH", "sql/source_query.sql"),
        target_table=require("TARGET_TABLE", "llm_analysis_results"),
        llm_base_url=os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1"),
        llm_model=os.getenv("LLM_MODEL", "gpt-oss-20b"),
        llm_api_key=os.getenv("LLM_API_KEY", "lm-studio"),
        llm_timeout_seconds=int(os.getenv("LLM_TIMEOUT_SECONDS", "180")),
        llm_max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1600")),
        llm_temperature=float(os.getenv("LLM_TEMPERATURE", "0.2")),
        llm_system_prompt_path=BASE_DIR / os.getenv("LLM_SYSTEM_PROMPT_PATH", "prompts/system_prompt.txt"),
        job_name=os.getenv("JOB_NAME", "redshift_llm_analysis_job"),
        source_name=os.getenv("SOURCE_NAME", "default_source_query"),
        source_preview_rows=int(os.getenv("SOURCE_PREVIEW_ROWS", "200")),
    )


def get_connection(config: Config):
    return redshift_connector.connect(
        host=config.redshift_host,
        port=config.redshift_port,
        database=config.redshift_database,
        user=config.redshift_user,
        password=config.redshift_password,
    )


def read_text(path: Path, fallback: str | None = None) -> str:
    if path.exists():
        return path.read_text(encoding="utf-8")
    if fallback is not None:
        return fallback
    raise FileNotFoundError(f"File not found: {path}")


def fetch_source_data(config: Config) -> tuple[str, list[dict[str, Any]]]:
    sql = read_text(config.source_sql_path)
    logger.info("Running source query from %s", config.source_sql_path)

    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"set search_path to {config.redshift_schema}")
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

    records = [dict(zip(columns, row)) for row in rows]
    logger.info("Fetched %s rows from Redshift", len(records))
    return sql, records


def build_user_prompt(config: Config, sql: str, records: list[dict[str, Any]]) -> str:
    preview = records[: config.source_preview_rows]
    payload = {
        "job_name": config.job_name,
        "source_name": config.source_name,
        "source_sql": sql,
        "row_count": len(records),
        "columns": list(preview[0].keys()) if preview else [],
        "records_preview": preview,
        "primary_metrics": [
            "sales",
            "transaction",
            "basket size",
            "speed of service",
            "transaction per manhour",
            "upsell rate",
        ],
        "note": (
            f"Only the first {len(preview)} rows are provided in records_preview. "
            f"Total row_count is {len(records)}."
        ),
    }
    return json.dumps(payload, default=str, ensure_ascii=False)


def call_llm(config: Config, user_prompt: str) -> tuple[str, dict[str, Any]]:
    system_prompt = read_text(config.llm_system_prompt_path, DEFAULT_SYSTEM_PROMPT)
    url = config.llm_base_url.rstrip("/") + "/chat/completions"
    payload = {
        "model": config.llm_model,
        "temperature": config.llm_temperature,
        "max_tokens": config.llm_max_tokens,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.llm_api_key}",
    }

    logger.info("Calling LM Studio model %s at %s", config.llm_model, url)
    response = requests.post(url, headers=headers, json=payload, timeout=config.llm_timeout_seconds)
    response.raise_for_status()

    body = response.json()
    content = body["choices"][0]["message"]["content"]
    parsed = json.loads(content)
    logger.info("Received analysis from LLM")
    return content, parsed


def insert_analysis(
    config: Config,
    source_sql: str,
    source_rows: list[dict[str, Any]],
    llm_raw_response: str,
    llm_analysis: dict[str, Any],
) -> str:
    run_id = str(uuid.uuid4())
    created_at = datetime.now(timezone.utc)
    insert_sql = f"""
        insert into {config.target_table} (
            run_id,
            job_name,
            source_name,
            source_sql,
            source_row_count,
            source_payload_json,
            analysis_headline,
            analysis_summary,
            analysis_json,
            llm_raw_response,
            created_at_utc
        ) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with get_connection(config) as conn:
        with conn.cursor() as cursor:
            cursor.execute(f"set search_path to {config.redshift_schema}")
            cursor.execute(
                insert_sql,
                (
                    run_id,
                    config.job_name,
                    config.source_name,
                    source_sql,
                    len(source_rows),
                    json.dumps(source_rows, default=str),
                    llm_analysis.get("headline"),
                    llm_analysis.get("summary"),
                    json.dumps(llm_analysis, default=str),
                    llm_raw_response,
                    created_at,
                ),
            )
        conn.commit()

    logger.info("Inserted analysis row into %s with run_id=%s", config.target_table, run_id)
    return run_id


def main() -> int:
    try:
        config = load_config()
        source_sql, source_rows = fetch_source_data(config)
        user_prompt = build_user_prompt(config, source_sql, source_rows)
        llm_raw_response, llm_analysis = call_llm(config, user_prompt)
        run_id = insert_analysis(config, source_sql, source_rows, llm_raw_response, llm_analysis)

        result = {
            "status": "success",
            "run_id": run_id,
            "row_count": len(source_rows),
            "target_table": config.target_table,
            "headline": llm_analysis.get("headline"),
        }
        print(json.dumps(result, indent=2))
        return 0
    except Exception as exc:
        logger.exception("Job failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
