{{ config (
    materialized="table"
)}}
{% set event_type %}
select 
event_type
from leap_staging.PUBLIC.leap_event_dup
{% endset %}

{% set query1 %}
select PARSE_JSON(payload) as json.from leap_staging.PUBLIC.leap_event_dup
{% endset %}
{% set query2 %}
select PARSE_JSON(payload:originalEntity) as json.from leap_staging.PUBLIC.leap_event_dup
{% endset %}


{% if  event_type == 'UPDATED' %}
    {% do run_query(query1) %}
    select 
    json:_aggregateId::string as id,
    json:dob::DATE as DOB,
    json:clientStatus:status::string as client_status,
    json:multipleBirthBoolean as MULTIPLE_BIRTH_BOOLEAN,
    json:contactPreference as CONTACT_PREFERENCE_CODE,
    json:gender:code as BIRTH_GENDER_CODE,
    json:gender:label::string as BIRTH_GENDER_NAME,
    json:organizationId::string as ORGANIZATION_ID
    from leap_staging.public.leap_event_dup
{% else %}
    {% do run_query(query2) %}
    select 
    json:_aggregateId::string as id,
    json:dob::DATE as DOB,
    json:clientStatus:status::string as client_status,
    json:multipleBirthBoolean as MULTIPLE_BIRTH_BOOLEAN,
    json:contactPreference as CONTACT_PREFERENCE_CODE,
    json:gender:code as BIRTH_GENDER_CODE,
    json:gender:label::string as BIRTH_GENDER_NAME,
    json:organizationId::string as ORGANIZATION_ID
    from leap_staging.public.leap_event_dup 
{% endif %}










{% set event_list = [] %}
    
    {%- for row in run_query(
                               "select 
                            event_type
                            from leap_staging.PUBLIC.leap_event_dup
                                "
    ) -%}
    {%- endfor -%}

{% if "UPDATED" in event_list %}
    SELECT PARSE_JSON(payload:originalEntity) as json,
    json:_aggregateId::string as id,
    json:dob::DATE as DOB,
    json:clientStatus:status::string as client_status,
    json:multipleBirthBoolean as MULTIPLE_BIRTH_BOOLEAN,
    json:contactPreference as CONTACT_PREFERENCE_CODE,
    json:gender:code as BIRTH_GENDER_CODE,
    json:gender:label::string as BIRTH_GENDER_NAME,
    json:organizationId::string as ORGANIZATION_ID
    from leap_staging.public.leap_event_dup 
{% else %}
    select 
    PARSE_JSON(payload) as json,
    json:_aggregateId::string as id,
    json:dob::DATE as DOB,
    json:clientStatus:status::string as client_status,
    json:multipleBirthBoolean as MULTIPLE_BIRTH_BOOLEAN,
    json:contactPreference as CONTACT_PREFERENCE_CODE,
    json:gender:code as BIRTH_GENDER_CODE,
    json:gender:label::string as BIRTH_GENDER_NAME,
    json:organizationId::string as ORGANIZATION_ID
    from leap_staging.public.leap_event_dup
{% endif %}