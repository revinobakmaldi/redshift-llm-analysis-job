create table if not exists public.llm_analysis_results (
    run_id varchar(64) not null,
    job_name varchar(255),
    source_name varchar(255),
    source_sql varchar(max),
    source_row_count integer,
    source_payload_json varchar(max),
    analysis_headline varchar(65535),
    analysis_summary varchar(max),
    analysis_json varchar(max),
    llm_raw_response varchar(max),
    created_at_utc timestamp,
    primary key (run_id)
);
