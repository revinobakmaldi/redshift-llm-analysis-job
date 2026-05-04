# Redshift → LM Studio → Redshift job

This Python job does 3 things:
1. pulls data from AWS Redshift
2. sends the data to your local LM Studio model (`gpt-oss-20b` by default) for QSR operations analysis
3. writes the analysis result back into Redshift

The analyst role is tuned for QSR operations and expects input data around:
- sales
- transaction
- basket size
- speed of service
- transaction per manhour
- upsell rate

## Files
- `main.py` - the job runner
- `.env.example` - environment variables to copy into `.env`
- `.gitignore` - keeps secrets and local virtualenv files out of git
- `sql/source_query.sql` - your source extraction query
- `sql/create_target_table.sql` - example writeback table
- `prompts/system_prompt.txt` - QSR operations analysis instructions for the model

## Setup
```bash
cd /root/.openclaw/workspace/projects/redshift-llm-analysis-job
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Then edit:
- `.env`
- `sql/source_query.sql`

## Run
```bash
python main.py
```

## Notes
- LM Studio must expose the OpenAI-compatible API, usually at `http://127.0.0.1:1234/v1`.
- If the job runs on a server but LM Studio runs on your PC, `127.0.0.1` will not work. Replace `LLM_BASE_URL` with your PC's reachable IP and allow inbound access.
- The writeback table is just a starter schema. Adjust it to match your warehouse standard.
- The job currently sends only a preview subset of rows to the model, controlled by `SOURCE_PREVIEW_ROWS`, while still recording the full row count.
