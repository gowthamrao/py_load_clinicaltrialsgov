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
    nct_id VARCHAR(255) NOT NULL,
    agency_class VARCHAR(255),
    name TEXT,
    is_lead BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_sponsors_nct_id ON sponsors(nct_id);

CREATE TABLE IF NOT EXISTS conditions (
    id SERIAL PRIMARY KEY,
    nct_id VARCHAR(255) NOT NULL,
    name TEXT
);
CREATE INDEX IF NOT EXISTS idx_conditions_nct_id ON conditions(nct_id);

CREATE TABLE IF NOT EXISTS interventions (
    id SERIAL PRIMARY KEY,
    nct_id VARCHAR(255) NOT NULL,
    intervention_type VARCHAR(255),
    name TEXT,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_interventions_nct_id ON interventions(nct_id);

CREATE TABLE IF NOT EXISTS design_outcomes (
    id SERIAL PRIMARY KEY,
    nct_id VARCHAR(255) NOT NULL,
    outcome_type VARCHAR(255),
    measure TEXT,
    time_frame TEXT,
    description TEXT
);
CREATE INDEX IF NOT EXISTS idx_design_outcomes_nct_id ON design_outcomes(nct_id);


CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id SERIAL PRIMARY KEY,
    nct_id VARCHAR(255),
    payload JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS load_history (
    id SERIAL PRIMARY KEY,
    load_timestamp TIMESTAMP,
    status VARCHAR(255),
    metrics JSONB
);


-- Staging tables are UNLOGGED for performance
CREATE UNLOGGED TABLE IF NOT EXISTS staging_raw_studies (
    nct_id VARCHAR(255),
    last_updated_api TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    payload JSONB
);

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

CREATE UNLOGGED TABLE IF NOT EXISTS staging_interventions (
    nct_id VARCHAR(255),
    intervention_type VARCHAR(255),
    name TEXT,
    description TEXT
);

CREATE UNLOGGED TABLE IF NOT EXISTS staging_design_outcomes (
    nct_id VARCHAR(255),
    outcome_type VARCHAR(255),
    measure TEXT,
    time_frame TEXT,
    description TEXT
);
