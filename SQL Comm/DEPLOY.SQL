SET ECHO ON
SET TRIMSPOOL ON
SET TRIMOUT ON
SET PAGESIZE 0
SET LINESIZE 2000
SET TERMOUT ON
SET SERVEROUTPUT ON SIZE UNLIMITED
SET DEFINE ON
SET VERIFY OFF
SET FEEDBACK ON

WHENEVER SQLERROR EXIT FAILURE
WHENEVER OSERROR EXIT FAILURE
SPOOL MDM_4.0.6.LOG


/***********************************************************/
/** Please add schema names according to your environment **/
/***********************************************************/

DEFINE MDM_USER=MSTRDATA;
DEFINE STG_USER=STG_MDM;
DEFINE STG_ODI_USER=STG_ODI_MDM;
DEFINE AGGSVC_USER=AGGSVC;

/***********************************************************/
ALTER SESSION SET CURRENT_SCHEMA = DEPLOYMENT;
@./ADMIN/PRIVILEGES_PRE.sql

ALTER SESSION SET CURRENT_SCHEMA = &STG_USER;
@./Staging/DDL/DDL.sql
@./Staging/DML/DML.sql
@./Staging/Views/V_STAGING_PERSON.sql
@./Staging/Packages/PKG_ODI.sql
@./Staging/Packages/PKG_COMMON.sql


ALTER SESSION SET CURRENT_SCHEMA = &MDM_USER;
@./Final/MDM_DB_History/R_MDM_DB_HISTORY.sql
@./Final/Packages/PKG_MERGE_ENTITY.sql
@./Final/DDL/DDL.sql
@./Final/DML/DML.sql
@./Final/Views/V_STAGING_PERSON.sql
@./Final/Views/V_STAGING_CLAIM.sql
@./Final/Views/V_CLAIM_DENORM.sql
@./Final/Views/V_ALL_CLAIM.sql
@./Final/Packages/PKG_COMMON.sql
@./Final/Packages/PKG_MDM_BUSINESS.sql
@./Final/Packages/PKG_MDM_PERSON.sql
@./Final/Packages/PKG_EDQ.sql
@./Final/Packages/PKG_MDM.sql
@./Final/Packages/PKG_EDQ_CLAIM.sql
@./Final/Packages/PKG_BACKFILL.sql
@./Final/Triggers/TR_PERSON_AUDIT.sql
@./Final/Triggers/TR_ADDRESS_AUDIT.SQL
@./Final/Triggers/TR_BUSINESS_ALIAS_AUDIT.SQL
@./Final/Triggers/TR_BUSINESS_AUDIT.SQL
@./Final/Triggers/TR_BUSINESS_SUB_TYPE_AUDIT.SQL
@./Final/Triggers/TR_BUSINESS_TO_BUSINESS_AUDIT.SQL
@./Final/Triggers/TR_BUSINESS_TO_PERSON_AUDIT.SQL
@./Final/Triggers/TR_CONNECTION_AUDIT.SQL
@./Final/Triggers/TR_PERSON_ALIAS_AUDIT.SQL
@./Final/Triggers/TR_CLAIM_ALIAS_AUDIT.sql
@./Final/Triggers/TR_CLAIM_AUDIT.sql
@./Final/Triggers/TR_CLAIM_ICD_CODES_AUDIT.sql
@./Final/Triggers/TR_PATIENT_ALIAS_EXT_AUDIT.sql
@./Final/Triggers/TR_PATIENT_AUDIT.sql


ALTER SESSION SET CURRENT_SCHEMA = &AGGSVC_USER;
@./AGGSVC/DDL/AGGSVC_DDL.sql
@./AGGSVC/Views/V_EVENT_ATTRIBUTE_METADATA.sql
@./AGGSVC/Views/V_EVENT_TYPE_METADATA.sql
@./AGGSVC/Views/V_EVENT_TO_EVENT_TYPE_METADATA.sql
@./AGGSVC/Views/V_PROVIDER_TYPE_METADATA.sql
@./AGGSVC/Views/V_PROVIDER_ATTRIBUTE_METADATA.sql
@./AGGSVC/Packages/PKG_COMMON.sql
@./AGGSVC/DML/DML.sql

ALTER SESSION SET CURRENT_SCHEMA = &MDM_USER;
@./Final/Packages/PKG_MDM_API.sql

ALTER SESSION SET CURRENT_SCHEMA = DEPLOYMENT;
@./ADMIN/PRIVILEGES_POST.sql

ALTER SESSION SET CURRENT_SCHEMA = &STG_USER;
@COMPILE.SQL &STG_USER
ALTER SESSION SET CURRENT_SCHEMA = &MDM_USER;
@COMPILE.SQL &MDM_USER
SPOOL OFF;