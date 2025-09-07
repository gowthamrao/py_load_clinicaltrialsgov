CREATE TABLE IF NOT EXISTS raw_studies (
    nct_id VARCHAR(255) PRIMARY KEY,
    last_updated_api TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    payload JSONB
);

CREATE TABLE IF NOT EXISTS studies (
    nct_id VARCHAR(255) PRIMARY KEY,
    brief_title TEXT,
    official_title TEXT,
    overall_status VARCHAR(255),
    start_date DATE,
    start_date_str VARCHAR(255),
    primary_completion_date DATE,
    primary_completion_date_str VARCHAR(255),
    study_type VARCHAR(255),
    brief_summary TEXT
);

CREATE TABLE IF NOT EXISTS sponsors (
    id SERIAL PRIMARY KEY,
    nct_id VARCHAR(255),
    agency_class VARCHAR(255),
    name TEXT,
    is_lead BOOLEAN
);

CREATE TABLE IF NOT EXISTS conditions (
    id SERIAL PRIMARY KEY,
    nct_id VARCHAR(255),
    name TEXT
);

CREATE TABLE IF NOT EXISTS load_history (
    id SERIAL PRIMARY KEY,
    load_timestamp TIMESTAMP,
    status VARCHAR(255),
    metrics JSONB
);

-- Staging tables
CREATE UNLOGGED TABLE IF NOT EXISTS staging_studies (
    nct_id VARCHAR(255),
    brief_title TEXT,
    official_title TEXT,
    overall_status VARCHAR(255),
    start_date DATE,
    start_date_str VARCHAR(255),
    primary_completion_date DATE,
    primary_completion_date_str VARCHAR(255),
    study_type VARCHAR(255),
    brief_summary TEXT
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging_sponsors (
    nct_id VARCHAR(255),
    agency_class VARCHAR(255),
    name TEXT,
    is_lead BOOLEAN
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging_conditions (
    nct_id VARCHAR(255),
    name TEXT
);
