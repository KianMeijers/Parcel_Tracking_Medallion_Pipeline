# System Context & Coding Guidelines for Data Engineering project
## Project Overview
The objective is to set up an end-to-end data pipeline that processes raw tracking data into a clean, validated open table format, which is then queried by an AI agent. We are prioritizing pragmatic, well-reasoned architecture over overcomplicated "AI slop".  

## Tech Stack & Constraints
Orchestration: Apache Airflow MUST be used to build the DAG.  
Storage: Data MUST land in open table format apache iceberg.  
Data Processing Architecture: The Medallion architecture (Bronze, Silver, Gold) MUST be utilized.  
Agent Interface: A simple terminal interface for the AI agent is sufficient; no fancy UI is required.  

## Architecture Definitions
- Bronze: Store the raw zipped JSON data as close to its original format as possible.  
- Silver: Consolidate the varying formats from the three different carriers into a unified, clean format.  
- Gold: Model the tables specifically to answer business questions, centering around calculating "transit time" (from carrier possession to delivery) and on-time rates based on SLAs.  

## Data Quirks & Edge Cases (CRITICAL)
When writing transformation logic, you must account for the following known data issues:
- Filenames vs. Event Times: A filename only indicates when a file was dumped, not when the actual events inside it occurred.  Retries: Carriers retry webhooks when acknowledgements time out, meaning duplicates exist.  
Mismatched Formats: The integrations were built years apart; units, time zones, and vocabulary will NOT agree. Normalization is required.  
- Bad Data: Some records will not parse. You must implement a strategy to handle or quarantine these gracefully.  
State Completeness: The dataset contains incomplete cases that may be completed by future files. Updates/merges must be handled correctly.  
- Idempotency: The pipeline refreshes daily; running the pipeline twice MUST NOT corrupt or duplicate the data.  

## AI Agent Guidelines
The agent sits on top of the Gold modeled data and must be able to answer questions in plain language.  

Target Queries to Support:
- How many parcels did organization 4471 ship in July 2026, broken down by carrier?  
- Which carrier has the worst on-time delivery rate for parcels over 5 kg?  
- Why did delivery performance drop in June?  

Safety & Scope: The agent MUST refuse questions it cannot answer using the provided dataset. Use read-only permissions for the LLM's database execution.  

## Best Practices
When generating code, please keep the following reliability features in mind:
Testing: Include code tests to verify transformation logic and data tests to automatically fail the job if anomalies are detected.  
Maintenance: Consider how to prevent query performance degradation from continuous data appends (e.g., table compaction/vacuuming).  