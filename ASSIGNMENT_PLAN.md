# NammaMart ETL & ELT Pipeline Assignment - Implementation Plan

## Overview
This plan outlines the steps to complete the NammaMart Quick Commerce ETL & ELT Pipeline Assignment. The goal is to build both ETL and ELT pipelines in Apache Airflow that deliver reconciled numbers by 9:00 AM IST daily, meeting all ten acceptance criteria.

## Prerequisites
1. Environment setup completed (Learner Setup Guide)
2. Dataset copied to `include/data/nammamart/`
3. Astro CLI and Docker Desktop installed
4. Git configured

## Phase 0: Discovery and Data Profiling

### Objectives
- Profile all six data files
- Check referential integrity
- Validate business rules
- Create Data Issues Log
- Prepare client questions

### Steps
1. **Data Profiling**
   - Write Python script to analyze:
     - Row counts
     - Nulls per column
     - Duplicate keys
     - Min/max of numeric/date columns
     - Distinct values of categorical columns
   - Execute against all CSV files

2. **Referential Integrity Checks**
   - Verify foreign key relationships:
     - orders.customer_id → customers.customer_id
     - orders.product_id → products.product_id
     - orders.store_id → stores.store_id
     - payments.order_id → orders.order_id

3. **Business Rule Validation**
   - Recompute order_amount and compare
   - Validate order_status values
   - Check payment_mode values
   - Verify delivery_minutes for DELIVERED orders
   - Check payment amount equals order_amount for SUCCESS payments
   - Validate increment file contains updates

4. **Data Issues Log Creation**
   - Document issues with:
     - File name
     - Row key(s)
     - Column
     - Problem description
     - Quality dimension
     - Proposed rule
     - Severity (Critical/Warning)

5. **Client Questions Preparation**
   - Prepare 3 questions based on data observations

## Phase A: ETL Pipeline Development

### Objectives
- Create `dags/nammamart_etl_<your_name>.py`
- Implement Extract → Validate → Transform → Load flow
- Achieve idempotent loading into star schema
- Build payment reconciliation
- Generate required reports

### Components to Develop
1. **Helper Modules** (in `include/nammamart/`):
   - `config.py`: Constants, paths, timezone settings
   - `rules.py`: Data quality validation functions
   - `masking.py`: PII masking functions
   - `etl.py`: Core ETL logic (extract, validate, transform, load)
   - `reconcile.py`: Payment reconciliation logic
   - `warehouse.py`: DuckDB connection handling
   - `runtime.py`: Airflow context helpers

2. **DAG Structure**:
   - 6 parallel extract tasks
   - Master data validation (stores, products, customers)
   - Orders and payments validation
   - Quarantine failed rows
   - Transform joined data
   - Load star schema (fact + dimension tables)
   - Reconcile payments
   - Build reports
   - Run audit (trigger_rule="all_done")

3. **Key Features**:
   - Parallel extraction
   - Quarantine with failure_reason
   - Idempotent fact table loading
   - PII masking
   - Star schema design
   - Payment reconciliation table
   - Daily zone revenue and category margin reports

## Phase B: ELT Pipeline Development

### Objectives
- Create `dags/nammamart_elt_<your_name>.py`
- Implement Extract → Load raw → Transform in warehouse
- Use raw/staging/mart layers
- Implement MERGE for incremental loads
- Ensure mart totals match ETL

### Components to Develop
1. **Helper Modules** (extend existing):
   - `elt.py`: Core ELT logic (load raw, stage tables, build marts)
   - SQL transformation files in `include/sql/elt/`

2. **DAG Structure**:
   - 6 parallel load_raw tasks
   - Staging tables for masters (stores, products, customers)
   - Staging orders with MERGE logic
   - Staging payments
   - 5 mart tables build tasks
   - Run audit (trigger_rule="all_done")

3. **Layers Implementation**:
   - **Raw Layer**: Exact CSV copies with metadata (_loaded_at, _source_file, _run_id)
   - **Staging Layer**: Typed, standardized, deduplicated, PII masked, with dq_status/failure_reason
   - **Mart Layer**: Business-ready tables answering the six questions

4. **Incremental Load**:
   - Raw: Append increment file
   - Staging: MERGE/UPSERT on order_id
   - Marts: Rebuild from staged data

## Phase C: Scheduling

### Objectives
- Schedule both DAGs for 9:00 AM IST Monday-Saturday
- Handle timezone properly
- Justify catchup=False
- Configure retries and timeouts

### Implementation
- Cron expression: `0 9 * * 1-6`
- Timezone-aware start_date using pendulum (Asia/Kolkata)
- catchup=False with justification (full reload each run)
- default_args: retries=2, retry_delay=1min, execution_timeout=10min
- dagrun_timeout=25min for buffer before 9:30 AM review

## Phase D: Data Quality Dimensions

### Objectives
- Implement at least one check for each of six dimensions
- Apply in both pipelines (Python for ETL, SQL for ELT)
- Assign severity (Critical quarantines, Warning logs)
- Populate dq_results table

### Dimensions and Example Checks
1. **Completeness**: Required data present
   - customer_id NOT NULL in orders
   - DELIVERED orders HAVE delivery_minutes
   - category NOT NULL in products

2. **Uniqueness**: Each record once
   - order_id unique
   - payment_id unique
   - One row per customer_id/product_id

3. **Validity**: Format/allowed list compliance
   - order_status/payment_mode in allowed lists
   - Email format, phone 10 digits, pincode 6 digits
   - discount_pct 0-100
   - order_ts parses as timestamp

4. **Accuracy**: Believable in real world
   - quantity > 0
   - order_amount matches formula
   - mrp > 0, cost_price <= mrp
   - delivery_minutes < 120

5. **Consistency**: Agreement with other data
   - FK relationships valid
   - Payments match orders and amounts
   - loyalty_tier consistent casing

6. **Timeliness**: Data current
   - No future-dated timestamps
   - Latest order not older than 1 day at runtime

### Implementation
- Create dq_results table with columns:
  - run_id, dag_id, check_name, dimension, severity, rows_checked, rows_failed, status, checked_at
- Implement checks in rules.py (ETL) and SQL files (ELT)
- Log every check result per run

## Phase E: Data Governance

### Objectives
- Answer 9 governance questions based on pipeline implementation
- Provide specific, pipeline-based answers

### Key Areas to Address
1. **Ownership**: Define who owns each dataset and approves rule changes
2. **Data Dictionary**: Document fact and dimension tables
3. **Lineage**: Trace a number from mart to source
4. **Personal Data**: Identify PII, masking approach, applicable law (DPDP Act 2023)
5. **Access Control**: Define who can access raw/staging/mart/quarantine
6. **Retention**: Specify retention policies for different data types
7. **Secrets**: Explain Variable.get() usage for credentials
8. **Audit**: Show how to trace a rejected order
9. **Change History**: Explain handling of order status changes over time

## Phase F: Observability Tests

### Objectives
- Implement run audit table
- Run 10 experiments on both DAGs
- Record Expected → Observed → Where Seen → What I Would Change
- Map to five pillars of data observability

### Required Components
1. **Run Audit Table**:
   - pipeline_runs with: run_id, dag_id, start_time, end_time, rows_extracted, rows_loaded, rows_quarantined, status

2. **Experiments to Run**:
   1. Happy path (normal trigger)
   2. Re-run (idempotency check)
   3. Incremental load (with increment file)
   4. Missing source file
   5. New bad data (inject known issues)
   6. Schema change (rename column)
   7. Volume anomaly (drastically reduced data)
   8. Distribution drift (artificially inflate values)
   9. Freshness test (old data)
   10. Recovery (fix failure and clear task)

### Observability Pillars Mapping
- **Schema**: Contract validation failures
- **Freshness**: Timeliness checks
- **Volume**: Row count monitoring
- **Distribution**: Value distribution checks
- **Lineage**: Ability to trace to source

## Phase G: Client Recommendation

### Objectives
- Write one-page recommendation to CTO Sameer
- Base on observed evidence, not theory
- Cover auditability, quality guarantees, ease of change, performance at scale, cost
- End with top 3 risks

### Recommendation Structure
1. **Recommendation Statement** (ETL vs ELT)
2. **Evidence-Based Justification**:
   - Auditability comparison
   - Quality guarantees demonstration
   - Ease of change analysis
   - Performance scaling expectations
   - Cost considerations
3. **Top 3 Risks** with mitigation strategies

## Submission Preparation

### Checklist Completion
1. ✓ Both DAGs parse and run without import errors
2. ✓ Helper code in `include/nammamart/` and SQL in `include/sql/`
3. ✓ SUBMISSION.md containing all required sections
4. ✓ Screenshots from Airflow UI:
   - Both DAGs in Graph view (green run)
   - Schedule showing 9:00 AM IST / 03:30 UTC
   - One quarantine file example
   - Query results from dq_results, pipeline_runs, payment reconciliation
5. ✓ Answers to all six business questions as SQL + results
6. ✓ No secrets/unmasked PII outside include/data/
7. ✓ Provide GitHub repository link

### Verification Steps
1. Start Airflow: `astro dev start`
2. Verify DAGs appear in UI
3. Trigger manual runs to test
4. Check quarantine files are created
5. Verify mart tables contain expected data
6. Run business questions queries
7. Execute observability experiments
8. Stop Airflow: `astro dev stop`

## Timeline Estimation
- Phase 0 (Discovery): 1-2 days
- Phase A (ETL): 2-3 days
- Phase B (ELT): 2-3 days
- Phase C (Scheduling): 0.5 days
- Phase D (Data Quality): 1-2 days
- Phase E (Governance): 1 day
- Phase F (Observability): 2-3 days
- Phase G (Recommendation): 0.5 days
- Submission & Verification: 1 day
- **Total**: 10-14 days

## Success Criteria
All ten acceptance criteria must be satisfied:
1. Report data ready before 9:00 AM IST, Mon-Sat
2. No bad order reaches finance tables
3. Every raw record kept for audit
4. Bad rows quarantined with reasons
5. No double-counting on re-run
6. Incremental load handles updates correctly
7. Orders reconcile with payments
8. Customer PII protected
9. Pipeline observability implemented
10. Written recommendation provided

## Risk Mitigation
1. **Data Complexity**: Start with profiling to understand issues early
2. **Pipeline Failures**: Implement comprehensive logging and error handling
3. **Time Zone Mistakes**: Use pendulum explicitly for IST
4. **Performance Issues**: Test with subset of data first
5. **Requirement Misinterpretation**: Regularly check against acceptance criteria

---
*Note: This plan assumes starting from scratch. Since some work appears to already be completed (based on existing files), focus should be on filling gaps, verifying completeness, and preparing for submission.*