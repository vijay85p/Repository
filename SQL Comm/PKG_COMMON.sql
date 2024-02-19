create or replace PACKAGE PKG_COMMON
AS
/**************************************************************************************
  Name: PKG_COMMON
  Description: Generic package contain routines for ODI processing and ODI Scheduler.

  REVISIONS:
  VER           DATE          AUTHOR            DESCRIPTION
  --------      ----------    ---------------   --------------------------------------------------------------------
  MDM_2.0.0                   Arun Kumar        Initial Version
  MDM_2.0.2                   Anshul Rastogi    Change the SP_GET_SCENARIO_DETAILS
  MDM_3.0.10    May 05, 2017  NAKSHATRA GUPTA   Changed the call to GATHER_STATS to have a DEGREE 16
  MDM_4.0.2    Jun 21, 2017  RAGHVENDRA   Changed the SP_REBUILD_ALL_INDEXES null functions
 ******************************************************************************************************************/
    --------------------------------------------
    --
    -- Routines
    --
    --------------------------------------------
    /*
        Rebuild All Indexes
    */
    PROCEDURE SP_REBUILD_ALL_INDEXES
    (
        PVI_TABLE_NAME VARCHAR2 DEFAULT NULL
    );
    PROCEDURE SP_GATHER_STATS_ALL_TABLES
    (
        PVI_TABLE_NAME VARCHAR2 DEFAULT NULL
    );
    PROCEDURE SP_GET_SCENARIO_DETAILS
    (
        PVI_VERSION IN VARCHAR2 DEFAULT '001',
        PVO_SCENARIO OUT VARCHAR2
    );
    PROCEDURE SP_UPDATE_JOB_STATUS
    (
      PNI_JOB_ID IN NUMBER,
      PVI_JOB_STATUS IN VARCHAR2,
      PNI_SESSION_ID IN NUMBER,
      PDI_JOB_RUN_TIME IN DATE,
      PVI_RUN_TIME_FLAG IN VARCHAR2
     );
     /*
        Error handler
     */
     PROCEDURE SP_ERROR_HANDLER
      ( PVI_SOURCE_REF IN VARCHAR2 DEFAULT NULL,
        PVI_SOURCE_NAME IN VARCHAR2 DEFAULT NULL,
        PVI_ENTITY_NAME IN VARCHAR2 DEFAULT NULL
       );
	/*
	  EDQ Job invocation based on EDQ_JOB_YN flag and Record count of ODI job run
	*/
	PROCEDURE SP_CHECK_EDQ_JOB
	(	PNI_ODIJOB_ID IN NUMBER,
		PNI_SESSION_ID IN NUMBER,
		PVO_EDQ_JOB OUT VARCHAR2
	);
END;
/

create or replace PACKAGE BODY PKG_COMMON
AS
/**************************************************************************************
  Name: PKG_COMMON
  Description: Generic package contain routines for ODI processing and ODI Scheduler.

  REVISIONS:
  VER           DATE          AUTHOR            DESCRIPTION
  --------      ----------    ---------------   ------------------------------------
  MDM_2.0.0             	  Arun Kumar        Initial Version
  MDM_2.0.2            		  Anshul Rastogi    Change the SP_GET_SCENARIO_DETAILS
  MDM_3.0.10    May 05, 2017  NAKSHATRA GUPTA   Changed the call to GATHER_STATS to have a DEGREE 16
  MDM_4.0.2    Jun 21, 2017  RAGHVENDRA   Changed the SP_REBUILD_ALL_INDEXES null functions
*************************************************************************************/
    --------------------------------------------
    --
    --  *Rebuild All Indexes
    --
    --------------------------------------------
    PROCEDURE SP_REBUILD_ALL_INDEXES
    (
        PVI_TABLE_NAME VARCHAR2 DEFAULT NULL
    )
    IS
    IS_REBUILD_INDEX VARCHAR2(10);
    BEGIN
      
        SELECT OBJECT_VALUE INTO IS_REBUILD_INDEX FROM MSTRDATA.C_APPLICATION_CONFIG WHERE OBJECT_NAME = 'DB' AND OBJECT_KEY='REBUILD_INDEX';
        
        IF (IS_REBUILD_INDEX = 'Y') THEN
          dbms_output.put_line('REBUILDING INDEX');
          FOR i IN (select 'ALTER INDEX '||INDEX_NAME||' REBUILD' AS sql_rebuild from user_indexes where index_type = 'NORMAL' AND TABLE_NAME = NVL(PVI_TABLE_NAME, TABLE_NAME))
          LOOP
              BEGIN
                  EXECUTE IMMEDIATE (i.sql_rebuild);
              EXCEPTION
                  WHEN OTHERS THEN
                    NULL;
              END;
          END LOOP;
        END IF;
    END;
    -------------------------------------------------
    -- Stats Gathering using DBMS_STATS Package
    --
    --------------------------------------------------
    PROCEDURE SP_GATHER_STATS_ALL_TABLES
    (
        PVI_TABLE_NAME VARCHAR2 DEFAULT NULL
    )     
    IS
    V_CURRENT_ROW_COUNT C_TABLE_STATS.ROW_COUNT%TYPE;
    V_NEW_ROW_COUNT C_TABLE_STATS.ROW_COUNT%TYPE;
    V_THRESHOLD C_TABLE_STATS.ROW_COUNT_THRESHOLD%TYPE;
    V_TABLE_STATS_RID C_TABLE_STATS.TABLE_STATS_RID%TYPE;
    V_SQL VARCHAR2(1000);
    V_ROW_EXISTS NUMBER(6);
    V_SCHEMA_NAME VARCHAR2(50);
    BEGIN
        
        V_SCHEMA_NAME := 'STG_MDM';
        SELECT 
          CASE WHEN EXISTS(SELECT 1 FROM C_TABLE_STATS WHERE TABLE_NAME=PVI_TABLE_NAME AND SCHEMA_NAME = V_SCHEMA_NAME)
              THEN 1
              ELSE 0 
          END INTO V_ROW_EXISTS
        FROM DUAL;
        
        IF (V_ROW_EXISTS = 1) THEN
          SELECT TABLE_STATS_RID,ROW_COUNT,ROW_COUNT_THRESHOLD INTO V_TABLE_STATS_RID,V_CURRENT_ROW_COUNT,V_THRESHOLD FROM C_TABLE_STATS WHERE TABLE_NAME=PVI_TABLE_NAME AND SCHEMA_NAME = V_SCHEMA_NAME;
          
          V_SQL := 'SELECT COUNT(*) FROM ' ||V_SCHEMA_NAME ||'.' ||PVI_TABLE_NAME;
          dbms_output.put_line(V_SQL);
          EXECUTE IMMEDIATE V_SQL INTO V_NEW_ROW_COUNT;
          dbms_output.put_line(V_NEW_ROW_COUNT);
          IF (ABS(V_NEW_ROW_COUNT-V_CURRENT_ROW_COUNT)>V_THRESHOLD) THEN
            DBMS_STATS.GATHER_TABLE_STATS(OWNNAME=>V_SCHEMA_NAME,TABNAME=>PVI_TABLE_NAME,CASCADE=>TRUE,ESTIMATE_PERCENT=>NULL,GRANULARITY=>'AUTO',METHOD_OPT=>'FOR ALL COLUMNS SIZE AUTO',DEGREE=>16);
            V_SQL := 'UPDATE C_TABLE_STATS SET ROW_COUNT=' ||V_NEW_ROW_COUNT ||',' ||'LAST_RUN_TS=CURRENT_TIMESTAMP'
                    ||' WHERE TABLE_STATS_RID=' ||V_TABLE_STATS_RID;
                    
             dbms_output.put_line(V_SQL);
            EXECUTE IMMEDIATE V_SQL;
            COMMIT;
          END IF;
        ELSIF (PVI_TABLE_NAME like 'GTT%') THEN
             dbms_output.put_line('Inside GTT');
            DBMS_STATS.GATHER_TABLE_STATS(OWNNAME=>V_SCHEMA_NAME,TABNAME=>PVI_TABLE_NAME,CASCADE=>TRUE,ESTIMATE_PERCENT=>NULL,GRANULARITY=>'AUTO',METHOD_OPT=>'FOR ALL COLUMNS SIZE AUTO',DEGREE=>16);       
        END IF;
    END;
    ----------------------------------------------------------------------------------------
    ---- Procedure to Get the Scenario Name and Version for the Least Due In Time (Minutes)
    ----------------------------------------------------------------------------------------
    PROCEDURE SP_GET_SCENARIO_DETAILS
    (
        PVI_VERSION IN VARCHAR2 DEFAULT '001',
        PVO_SCENARIO OUT VARCHAR2
    ) IS

    BEGIN
          WITH cteStep1 AS
          (
              SELECT JOB_RID,
                     JOB_NAME,
                    ROUND(((DUE_DAY_IN_SECONDS+DUE_HOUR_IN_SECONDS+DUE_MINUTES_IN_SECONDS+DUE_SECONDS)/60),0) "DUE_TIME(Minutes)",
                     ROW_NUMBER() OVER (ORDER BY (DUE_DAY_IN_SECONDS+DUE_HOUR_IN_SECONDS+DUE_MINUTES_IN_SECONDS+DUE_SECONDS) ASC) AS SEQ_NUM
              FROM (
                      SELECT
                            a.JOB_RID,
                            a.JOB_NAME,
                            a."FREQUENCY(Minutes)" AS FREQUENCY,
                            EXTRACT (DAY FROM((NVL(b.LAST_RUN_TIME,TO_DATE('01-JAN-1997')) + a."FREQUENCY(Minutes)"/1440) - CAST(SYSTIMESTAMP AS TIMESTAMP)))*24*60*60 DUE_DAY_IN_SECONDS,
                            EXTRACT (HOUR FROM((NVL(b.LAST_RUN_TIME,TO_DATE('01-JAN-1997')) + a."FREQUENCY(Minutes)"/1440) - CAST(SYSTIMESTAMP AS TIMESTAMP)))*60*60 DUE_HOUR_IN_SECONDS,
                            EXTRACT (MINUTE FROM((NVL(b.LAST_RUN_TIME,TO_DATE('01-JAN-1997')) + a."FREQUENCY(Minutes)"/1440) - CAST(SYSTIMESTAMP AS TIMESTAMP)))*60 DUE_MINUTES_IN_SECONDS,
                            EXTRACT (SECOND FROM((NVL(b.LAST_RUN_TIME,TO_DATE('01-JAN-1997')) + a."FREQUENCY(Minutes)"/1440) - CAST(SYSTIMESTAMP AS TIMESTAMP))) DUE_SECONDS
                        FROM C_ODI_JOB_METADATA a
                        LEFT JOIN (SELECT JOB_ID,MAX(LAST_RUN_TIME) LAST_RUN_TIME
                                     FROM R_ODI_JOB_RUN_DETAILS
                                     GROUP BY JOB_ID
                                     ORDER BY JOB_ID
                                    ) b
                         ON (a.JOB_RID = b.JOB_ID)
                         WHERE a.IS_ACTIVE = 'Y'
                         --AND   a.STATUS NOT IN ('FAILED_ODI','FAILED_EDQ')
                    )
            )
            SELECT JOB_RID||'|'||JOB_NAME||'|'||PVI_VERSION||'|'||DECODE(SIGN("DUE_TIME(Minutes)"),-1,0,"DUE_TIME(Minutes)")
            INTO  PVO_SCENARIO
            FROM cteStep1 WHERE SEQ_NUM = 1;

    END;
      ---------------------------------------------------------------------------------------
      -- Procedure to Update the Job_Status in C_ODI_JOB_METADATA
      ---------------------------------------------------------------------------------------
      PROCEDURE SP_UPDATE_JOB_STATUS
      (
        PNI_JOB_ID IN NUMBER,
        PVI_JOB_STATUS IN VARCHAR2,
        PNI_SESSION_ID IN NUMBER,
        PDI_JOB_RUN_TIME IN DATE,
        PVI_RUN_TIME_FLAG IN VARCHAR2
      ) IS
      BEGIN

         IF(PVI_RUN_TIME_FLAG = 'TRUE') THEN
              UPDATE C_ODI_JOB_METADATA
              SET STATUS = PVI_JOB_STATUS,
                  JOB_START_DATE = PDI_JOB_RUN_TIME,
                  JOB_END_DATE  = NULL
               WHERE JOB_RID = PNI_JOB_ID;
              COMMIT;
         ELSE
              UPDATE C_ODI_JOB_METADATA
              SET STATUS = PVI_JOB_STATUS,
                  JOB_END_DATE = PDI_JOB_RUN_TIME
              WHERE JOB_RID = PNI_JOB_ID;
              COMMIT;

         END IF;

      END;
      -------------------------------------------------------------------------------
      --- Error Handling Procedure
      -------------------------------------------------------------------------------
      PROCEDURE SP_ERROR_HANDLER
      ( PVI_SOURCE_REF IN VARCHAR2 DEFAULT NULL,
        PVI_SOURCE_NAME IN VARCHAR2 DEFAULT NULL,
        PVI_ENTITY_NAME IN VARCHAR2 DEFAULT NULL
       )
      IS
         PRAGMA AUTONOMOUS_TRANSACTION;
         l_code   PLS_INTEGER := SQLCODE;
         l_mesg  VARCHAR2(32767) := SQLERRM;
      BEGIN
         INSERT INTO R_MDM_ERROR_LOG(ERROR_LOG_RID
                          ,SOURCE_REF
                          ,SOURCE_NAME
                          ,ENTITY_NAME
                          ,ERROR_CODE
                          ,ERROR_MESSAGE
                          ,BACKTRACE
                          ,CALLSTACK
                          ,CREATION_TS
                          ,CREATED_BY_ID
                         )
                      VALUES ( SEQ_MDM_ERROR_LOG.NEXTVAL
                          , PVI_SOURCE_REF
                          , PVI_SOURCE_NAME
                          , PVI_ENTITY_NAME
                          , l_code
                          ,  l_mesg
                          ,  DBMS_UTILITY.format_error_backtrace
                          ,  DBMS_UTILITY.format_call_stack
                          ,  CURRENT_TIMESTAMP
                           ,  USER
                          );

         COMMIT;
      END;
	 -----------------------------------------------------------------------------
	 -- To Check EDQ_JOB_YN flag and record count of ODI job run
	 -----------------------------------------------------------------------------
	PROCEDURE SP_CHECK_EDQ_JOB
	(	PNI_ODIJOB_ID IN NUMBER,
		PNI_SESSION_ID IN NUMBER,
		PVO_EDQ_JOB OUT VARCHAR2
	)
	IS
	V_EDQ_FLAG		      C_ODI_JOB_METADATA.EDQ_JOB_YN%TYPE;
	V_CNT			          NUMBER;
  V_INSERTED_RECORDS  NUMBER(12,0);
  V_UPDATED_RECORDS   NUMBER(12,0);
  V_DELETED_RECORDS   NUMBER(12,0);
	BEGIN
			BEGIN
				SELECT EDQ_JOB_YN
				INTO V_EDQ_FLAG
				FROM C_ODI_JOB_METADATA
				WHERE JOB_RID = PNI_ODIJOB_ID;
				EXCEPTION WHEN OTHERS THEN
					RAISE_APPLICATION_ERROR (-20001,SQLCODE||'-'||SQLERRM);
			END;
      BEGIN
           SELECT  NVL(NB_INS,0),
                   NVL(NB_UPD,0),
                   NVL(NB_DEL,0)
           INTO    V_INSERTED_RECORDS,
                   V_UPDATED_RECORDS,
                   V_DELETED_RECORDS
           FROM    SNP_SESSION
           WHERE   SESS_NO = PNI_SESSION_ID;
           EXCEPTION
                   WHEN OTHERS THEN
                   RAISE_APPLICATION_ERROR(-20001,SQLCODE ||'-'||SQLERRM);
      END;
      UPDATE R_ODI_JOB_RUN_DETAILS
      SET       INSERTED_RECORDS =   V_INSERTED_RECORDS,
                UPDATED_RECORDS  =   V_UPDATED_RECORDS,
                DELETED_RECORDS  =   V_DELETED_RECORDS
      WHERE   JOB_RUN_RID = PNI_SESSION_ID;
      COMMIT;
			BEGIN
				SELECT	COUNT(*)
				INTO	V_CNT
				FROM	R_ODI_JOB_RUN_DETAILS
				WHERE	JOB_ID = PNI_ODIJOB_ID
				AND		JOB_RUN_RID = PNI_SESSION_ID
				AND     (NVL(INSERTED_RECORDS,0) > 0 OR NVL(UPDATED_RECORDS,0) > 0 OR NVL(DELETED_RECORDS,0) > 0);
				EXCEPTION WHEN OTHERS THEN
					RAISE_APPLICATION_ERROR (-20001,SQLCODE||'-'||SQLERRM);
			END;
			PVO_EDQ_JOB := V_EDQ_FLAG||'|'||NVL(V_CNT,0);
	END;
END;
/
