{{ config (
    materialized="table"
)}}
with SAMPLE_EHR_MEBER AS (
    select 
        UUID_STRING() AS UNIQUE_ID,
        DATA_SOURCE_ID,
        LOADED_AT,
        FILE_NAME,
        FILE_TYPE,
        CREATE_DATE,
        CREATE_TIME,
        NUMBER_OF_RECORDS,
        ORGANIZATION_ID,
        MRN,CREATE_AT, UPDATED_AT
from LEAP_STAGING.PUBLIC.TEST_EHR_MEMBER
)
select * from SAMPLE_EHR_MEMBER