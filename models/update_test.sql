{{ config( materialized='plain') }}
-- ALTER SESSION SET TIMESTAMP_INPUT_FORMAT="YYYY-MM-DD HH24:MI:SS";

{% set col = var("cols") %}ls
{% set selcols = [] %}
{% for key, val in col.items() %}
    {%if "TRY_TO_TIMESTAMP" in val|string%}
        {%set rightval = (val|string|replace('"',"'"))%}
    {%else%}
        {%set rightval = "'"+(val|string)+"'"%}
    {%endif%}        
    {% set keyval = key|string+"="+rightval %}
    {%set ret = selcols.append(keyval)%}
{% endfor %}  

UPDATE LEAP_CIN_ANALYTICS.PUBLIC.CLIENT_DUP t1
SET {{ selcols|join(',') }}
FROM (select
  event_type,
  json:_aggregateId::string as ID,
  json:organizationId::string as ORGANIZATION_ID,
  json:_aggregateId::string as MASTER_PATIENT_ID,
  json:gender:code::string as BIRTH_GENDER_CODE,
  json:gender:label::string as BIRTH_GENDER_NAME,
  json:sexualOrientation.code::string as SEXUAL_ORIENTATION_CODE,
  json:sexualOrientation.name::string as SEXUAL_ORIENTATION_NAME,
  json:pronoun.name::string as PREFERRED_PRONOUNS,
  json:dob::DATE as DOB,
  json:clientStatus:code::string as CLIENT_STATUS,
  json:maritalStatus.code::string as MARITAL_STATUS_CODE,
  json:maritalStatus.name::string as MARITAL_STATUS_NAME,  
  json:contactPreference:code::string as CONTACT_PREFERENCE_CODE,
  json:contactPreference:name::string as CONTACT_PREFERENCE_NAME,
  json:educationLevel:name::string as HIGHEST_EDUCATION_LEVEL,
  TRY_TO_TIMESTAMP(json:createdAt::varchar) as CREATED_AT,
  TRY_TO_TIMESTAMP(json:updatedAt::varchar) as UPDATED_AT
    from (select event_type,
          PARSE_JSON(payload:updatedEntity) as json
          from LEAP_STAGING.PUBLIC.LEAP_EVENT_DUP t2 where event_type = 'UPDATED'))
    WHERE t1.ID='t2.ID'