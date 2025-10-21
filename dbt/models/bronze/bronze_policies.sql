{{ config(
    materialized='table',
    file_format='delta',
    tags=['bronze', 'policies', 'raw']
) }}

WITH source_data AS (
    SELECT 
        policy_id,
        customer_id,
        policy_number,
        policy_type,
        premium_amount,
        deductible_amount,
        coverage_limit,
        start_date,
        end_date,
        policy_status,
        agent_id,
        created_at,
        updated_at,
        -- Add metadata columns
        current_timestamp() AS ingestion_timestamp,
        '{{ run_started_at }}' AS dbt_run_timestamp,
        '{{ invocation_id }}' AS dbt_invocation_id
    FROM {{ source('raw', 'policies') }}
)

SELECT * FROM source_data
