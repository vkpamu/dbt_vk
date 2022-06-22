{{ config( materialized='plain') }}
insert into LEAP_CIN_ANALYTICS.PUBLIC.CLIENT_DUP("ID",
"ORGANIZATION_ID",
"MASTER_PATIENT_ID",
"BIRTH_GENDER_CODE",
"BIRTH_GENDER_NAME",
"SEXUAL_ORIENTATION_CODE",
"SEXUAL_ORIENTATION_NAME",
"PREFERRED_PRONOUNS",
"DOB",
"CLIENT_STATUS",
"MARITAL_STATUS_CODE",
"MARITAL_STATUS_NAME",  
"CONTACT_PREFERENCE_CODE",
"CONTACT_PREFERENCE_NAME",
"HIGHEST_EDUCATION_LEVEL",
"CREATED_AT",
"UPDATED_AT")(
  select
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
      case 
      WHEN event_type = 'UPDATED' THEN PARSE_JSON(payload:updatedEntity)
      WHEN event_type = 'CREATED' THEN PARSE_JSON(payload) END as json 
      from LEAP_STAGING.PUBLIC.LEAP_EVENT_DUP)
)