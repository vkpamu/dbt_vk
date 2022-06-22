{{ config( materialized='incremental',unique_key='CLIENT_MEDICAIDID',incremental_strategy='merge') }}
WITH using_clause AS (

    SELECT
    	UNIQUE_ID AS ID,
        PATIENT_FIRSTNAME AS FIRST,
        PATIENT_LASTNAME AS LAST,
        PATIENT_MIDDLENAME AS MIDDLE,
        UPDATED_AT,
    	CLIENT_MEDICAIDID
    FROM PATIENT_DEMOGRAPHIC_DATA

),
updates AS (
    SELECT
        ID,
        FIRST,
        LAST,
        MIDDLE,
        UPDATED_AT,
    	CLIENT_MEDICAIDID
    FROM using_clause

),

inserts AS (
    SELECT
        ID,
        FIRST,
        LAST,
        MIDDLE,
        UPDATED_AT,
    	CLIENT_MEDICAIDID
    FROM using_clause

)

SELECT * 
FROM updates 
