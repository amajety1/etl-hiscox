{{ config(
    materialized='table',
    file_format='delta',
    tags=['bronze', 'claims', 'raw']
) }}

WITH source_data AS (
    SELECT 
        claim_id,
        policy_id,
        customer_id,
        claim_amount,
        claim_date,
        claim_type,
        claim_status,
        description,
        adjuster_id,
        settlement_amount,
        settlement_date,
        created_at,
        updated_at,
        -- Add metadata columns
        current_timestamp() AS ingestion_timestamp,
        '{{ run_started_at }}' AS dbt_run_timestamp,
        '{{ invocation_id }}' AS dbt_invocation_id
    FROM {{ source('raw', 'claims') }}
)

SELECT * FROM source_data
