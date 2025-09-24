# ClinicalTrials.gov Data Source

This document describes the data source for the clinical trials ETL pipeline. The data is sourced from [ClinicalTrials.gov](https://clinicaltrials.gov/), a database of privately and publicly funded clinical studies conducted around the world.

ClinicalTrials.gov is a web-based resource that provides patients, their family members, health care professionals, researchers, and the public with easy access to information on publicly and privately supported clinical studies on a wide range of diseases and conditions. The U.S. National Library of Medicine (NLM) at the National Institutes of Health (NIH) maintains this site.

The information in ClinicalTrials.gov is provided and updated by the sponsor or principal investigator of the clinical study. Studies are generally submitted to the website (that is, registered) when they begin, and the information on the site is updated throughout the study. In some cases, results of the study are submitted after the study is completed.

## Database Schema

The ETL pipeline extracts data from ClinicalTrials.gov and stores it in a relational database with the following tables:

### `studies`

This table contains the main information about each clinical study.

| Column | Description |
| --- | --- |
| `nct_id` | The unique identifier for the study on ClinicalTrials.gov. |
| `brief_title` | A short title of the study. |
| `official_title` | The full, official title of the study. |
| `overall_status` | The overall recruitment status of the study (e.g., "Recruiting", "Completed"). |
| `start_date` | The date on which the study is anticipated to begin. |
| `start_date_str` | The string representation of the start date. |
| `primary_completion_date` | The date on which the last participant in a clinical study was examined or received an intervention to collect final data for the primary outcome measure. |
| `primary_completion_date_str` | The string representation of the primary completion date. |
| `study_type` | The type of study (e.g., "Interventional", "Observational"). |
| `brief_summary` | A brief summary of the study. |

### `sponsors`

This table contains information about the sponsors of the clinical study.

| Column | Description |
| --- | --- |
| `id` | A unique identifier for each sponsor record. |
| `nct_id` | The unique identifier for the study on ClinicalTrials.gov. |
| `agency_class` | The class of the agency (e.g., "NIH", "Industry"). |
| `name` | The name of the sponsor. |
| `is_lead` | A boolean value indicating whether the sponsor is the lead sponsor. |

### `conditions`

This table contains information about the conditions or diseases being studied in the clinical trial.

| Column | Description |
| --- | --- |
| `id` | A unique identifier for each condition record. |
| `nct_id` | The unique identifier for the study on ClinicalTrials.gov. |
| `name` | The name of the condition or disease. |

### `interventions`

This table contains information about the interventions being studied in the clinical trial.

| Column | Description |
| --- | --- |
| `id` | A unique identifier for each intervention record. |
| `nct_id` | The unique identifier for the study on ClinicalTrials.gov. |
| `intervention_type` | The type of intervention (e.g., "Drug", "Device"). |
| `name` | The name of the intervention. |
| `description` | A description of the intervention. |

### `design_outcomes`

This table contains information about the outcome measures of the clinical trial.

| Column | Description |
| --- | --- |
| `id` | A unique identifier for each design outcome record. |
| `nct_id` | The unique identifier for the study on ClinicalTrials.gov. |
| `outcome_type` | The type of outcome measure (e.g., "Primary", "Secondary"). |
| `measure` | The name of the outcome measure. |
| `time_frame` | The time frame over which the outcome measure is assessed. |
| `description` | A description of the outcome measure. |

## Data Relationships

The `studies` table is the central table in the database. The `nct_id` column is the primary key of the `studies` table and is used to link to the other tables in the database.

The `sponsors`, `conditions`, `interventions`, and `design_outcomes` tables are all child tables of the `studies` table. They are linked to the `studies` table by the `nct_id` column. Each of these tables can have multiple records for a single study.

## Data Provenance

The data in this database is sourced from [ClinicalTrials.gov](https://clinicaltrials.gov/). The data is downloaded from the ClinicalTrials.gov API and then transformed and loaded into the database.

For more detailed information about the data elements, please refer to the [AACT Data Dictionary](https://aact.ctti-clinicaltrials.org/data_dictionary).
