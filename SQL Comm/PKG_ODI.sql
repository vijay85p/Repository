create or replace PACKAGE  "PKG_ODI" AS
/**************************************************************************************
  Name: PKG_ODI
  Description: Package contains rountines for ODI Jobs.

  REVISIONS:
  VER           DATE        AUTHOR            DESCRIPTION
  --------      ----------  ---------------   --------------------------------------------------------------------
  MDM_1.0.0                  Arun Kumar        Initial Version
  MDM_2.0.25   27-SEP-2016   ANSHUL RASTOGI    Odi CITY-STATE STRIPPING CHANGES
 ******************************************************************************************************************/
 PROCEDURE SP_ODI_CREATE_INDEX
     (
        PVI_ODI_JOB_NAME IN VARCHAR2,
        PNI_SESS_NUMBER NUMBER,
        PNI_JOB_ID NUMBER
     );
      PROCEDURE SP_ODI_DROP_INDEX
     (
        PVI_ODI_JOB_NAME IN VARCHAR2,
        PNI_SESS_NUMBER NUMBER
     );
         ---------------------->> To get the ODI Session Id
      FUNCTION SF_GET_SESSION_ID
      (
        PVI_JOB_NAME IN VARCHAR2
      ) RETURN VARCHAR2;
	  ---------------------->> To get the ODI JOB Last run time
      FUNCTION SF_GET_ODI_JOB_LAST_RUN_TIME
      (
        PVI_JOB_NAME IN VARCHAR2
      ) RETURN VARCHAR2;

	  FUNCTION SF_GET_ODI_JOB_LAST_RUN_TS
      (
        PVI_JOB_NAME IN VARCHAR2
      ) RETURN VARCHAR2;
      ----------------------->> To Save the ODI JOB Last run time
      PROCEDURE SP_SAVE_ODI_JOB_RUN_DETAILS
      (
        PNI_JOB_RUN_ID IN NUMBER
      , PVI_JOB_NAME IN VARCHAR2
      , PVI_STEP_FLAG IN VARCHAR2
      , PVI_LAST_RUN_TIME IN VARCHAR2
      , PVI_JOB_STATUS IN VARCHAR2
      , PVI_RUN_USER IN VARCHAR2
      );
      PROCEDURE SP_UPDATE_ODI_JOB_RUN_DETAILS
      (
        PNI_JOB_RUN_ID IN NUMBER
      , PVI_JOB_NAME IN VARCHAR2
      , PVI_LAST_RUN_TIME IN VARCHAR2
      , PVI_JOB_STATUS IN VARCHAR2
      , PVI_RUN_USER IN VARCHAR2
      , PVI_INSERT_COUNT IN NUMBER
      , PVI_UPDATE_COUNT IN NUMBER
      , PVI_END_JOB_TIME IN TIMESTAMP
      , PVI_OUT_INSERT_COUNT OUT NUMBER
      );
     -------------------->> To call SOAP Request
     PROCEDURE SP_ADDRESS_API_SOAP_REQUEST
      (PVI_SOURCE_REF IN VARCHAR2,
       PVI_SOURCE_NAME IN VARCHAR2,
       PVI_ENTITY_NAME IN VARCHAR2,
       PVI_ADDRESS IN VARCHAR2,
       PVI_CITY    IN VARCHAR2,
       PVI_STATE   IN VARCHAR2,
       PVI_ZIP     IN VARCHAR2,
       PVI_ADDRESS_TYPE IN VARCHAR2
      );
      -------------------------->> To Populate address details in staging
      PROCEDURE SP_UPDATE_STG_ADDRESS_DETAILS;
      ------------------------->> Wrapper (TO Save ADDRESS API Data)
     PROCEDURE SP_SAVE_ADDRESS_API
     ( PVI_ODI_PACKAGE_NAME IN VARCHAR2
     );
     ---------------------------->> Wrapper (To Save Services Data)
     /*PROCEDURE SP_SAVE_SERVICE_DATA
     (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
     );
     --------------------------------->> Service Data PreLoad Setup
     PROCEDURE SP_SERVICE_DATA_PRELOAD
     (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
     );*/
     ----------------------------------->> MDM ODI Postload Setup
     PROCEDURE SP_MDM_DATA_POSTLOAD
     (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
     );


     PROCEDURE SP_RM_STR_BRANCH_COMP_NAME
     (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
     );


     PROCEDURE SP_ODI_DATA_VALIDATION
     (
        PVI_ENTITY_NAME IN C_ODI_ENTITY_METADATA.ENTITY_NAME%TYPE,
        PVI_SOURCE_NAME IN VARCHAR2
     );
END;
/

create or replace PACKAGE BODY PKG_ODI AS
/**************************************************************************************
  Name: PKG_ODI
  Description: Package contains rountines for ODI Jobs.

  REVISIONS:
  VER           DATE        AUTHOR            DESCRIPTION
  --------      ----------  ---------------   --------------------------------------------------------------------
  MDM_1.0.0                  Arun Kumar        Initial Version
  MDM_2.0.25   27-SEP-2016   ANSHUL RASTOGI    Odi STRIPPING CHANGES (MDM-973)
 ******************************************************************************************************************/
 FUNCTION SF_GET_SESSION_ID
      (
        PVI_JOB_NAME IN VARCHAR2
      ) RETURN VARCHAR2 AS
      V_SESSION_ID  VARCHAR2(256);
      V_COUNT    NUMBER;
  BEGIN
    BEGIN
          Select COUNT(*)
          INTO V_COUNT
          FROM (SELECT RANK() OVER (ORDER BY SESS_NO DESC) rank, SESS_NO FROM STG_MDM.SNP_SESSION WHERE SESS_NAME = PVI_JOB_NAME )  WHERE rank=2;
    END;
    IF V_COUNT > 0 THEN
      BEGIN
         Select SESS_NO INTO V_SESSION_ID
         FROM
         (SELECT RANK() OVER (ORDER BY SESS_NO DESC) rank, SESS_NO FROM STG_MDM.SNP_SESSION WHERE SESS_NAME = PVI_JOB_NAME )
         WHERE rank=2;
          EXCEPTION WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20001,'SESSION_ID NOT FOUND FOR JOB '||PVI_JOB_NAME);
      END;
    END IF;

    IF V_COUNT = 0 THEN
		BEGIN
			SELECT SESS_NO
			INTO V_SESSION_ID
      FROM
         (SELECT RANK() OVER (ORDER BY SESS_NO DESC) rank, SESS_NO FROM STG_MDM.SNP_SESSION WHERE SESS_NAME = PVI_JOB_NAME )
         WHERE rank=2;
         	EXCEPTION WHEN NO_DATA_FOUND THEN
			V_SESSION_ID := '111111';
		END;
    END IF;
        RETURN V_SESSION_ID;
  END SF_GET_SESSION_ID;

  FUNCTION SF_GET_ODI_JOB_LAST_RUN_TIME
      (
        PVI_JOB_NAME IN VARCHAR2
      ) RETURN VARCHAR2 AS
      V_LAST_RUN_TIME  VARCHAR2(256);
      V_COUNT    NUMBER;
  BEGIN
    BEGIN
          SELECT COUNT(*)
          INTO   V_COUNT
          FROM   R_ODI_JOB_RUN_DETAILS
          WHERE  JOB_ID = (SELECT JOB_RID FROM C_ODI_JOB_METADATA WHERE JOB_NAME = PVI_JOB_NAME)
		  AND    JOB_RUN_STATUS = 'DONE';
    END;
    IF V_COUNT > 0 THEN
      BEGIN
          SELECT MAX(TO_CHAR(LAST_RUN_TIME,'YYYYMMDDHH24MISS'))
          INTO   V_LAST_RUN_TIME
          FROM   R_ODI_JOB_RUN_DETAILS
          WHERE  JOB_ID = (SELECT JOB_RID FROM C_ODI_JOB_METADATA WHERE JOB_NAME = PVI_JOB_NAME)
		  AND    JOB_RUN_STATUS = 'DONE';
          EXCEPTION WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20001,'LAST RUN TIME NOT FOUND FOR THE JOB '||PVI_JOB_NAME);
      END;
    END IF;

    IF V_COUNT = 0 THEN
		BEGIN
			SELECT TO_CHAR(DEFAULT_LAST_RUN_DATE,'YYYYMMDDHH24MISS')
			INTO V_LAST_RUN_TIME
			FROM C_ODI_JOB_METADATA
			WHERE JOB_NAME = PVI_JOB_NAME;
			EXCEPTION WHEN NO_DATA_FOUND THEN
			V_LAST_RUN_TIME := to_char(to_date('1900-01-01','YYYY-MM-DD'),'YYYYMMDDHH24MISS');
		END;
    END IF;
        RETURN V_LAST_RUN_TIME;
  END SF_GET_ODI_JOB_LAST_RUN_TIME;

    FUNCTION SF_GET_ODI_JOB_LAST_RUN_TS
      (
        PVI_JOB_NAME IN VARCHAR2
      ) RETURN VARCHAR2 AS
      V_LAST_RUN_TIME  VARCHAR2(256);
      V_COUNT    NUMBER;
  BEGIN
    BEGIN
          SELECT COUNT(*)
          INTO   V_COUNT
          FROM   R_ODI_JOB_RUN_DETAILS
          WHERE  JOB_ID = (SELECT JOB_RID FROM C_ODI_JOB_METADATA WHERE JOB_NAME = PVI_JOB_NAME)
		  AND    JOB_RUN_STATUS = 'DONE';
    END;
    IF V_COUNT > 0 THEN
      BEGIN
          SELECT MAX(TO_CHAR(LAST_RUN_TIME,'YYYY/MM/DD HH24:MI:SS'))
          INTO   V_LAST_RUN_TIME
          FROM   R_ODI_JOB_RUN_DETAILS
          WHERE  JOB_ID = (SELECT JOB_RID FROM C_ODI_JOB_METADATA WHERE JOB_NAME = PVI_JOB_NAME)
		  AND    JOB_RUN_STATUS = 'DONE';
          EXCEPTION WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20001,'LAST RUN TIME NOT FOUND FOR THE JOB '||PVI_JOB_NAME);
      END;
    END IF;

    IF V_COUNT = 0 THEN
		BEGIN
			SELECT TO_CHAR(DEFAULT_LAST_RUN_DATE,'YYYY/MM/DD HH24:MI:SS')
			INTO V_LAST_RUN_TIME
			FROM C_ODI_JOB_METADATA
			WHERE JOB_NAME = PVI_JOB_NAME;
			EXCEPTION WHEN NO_DATA_FOUND THEN
			V_LAST_RUN_TIME := to_char(to_date('1900-01-01','YYYY-MM-DD'),'YYYY/MM/DD HH24:MI:SS');
		END;
    END IF;
        RETURN V_LAST_RUN_TIME;
  END SF_GET_ODI_JOB_LAST_RUN_TS;



 PROCEDURE SP_ODI_CREATE_INDEX(
    PVI_ODI_JOB_NAME IN VARCHAR2,
    PNI_SESS_NUMBER NUMBER,
    PNI_JOB_ID NUMBER)
    IS
    SQL_STMT1 VARCHAR2(200);
    V_CHECK NUMBER;
    BEGIN
 /*   SELECT COUNT(*) INTO V_CHECK FROM R_ODI_JOB_RUN_DETAILS WHERE JOB_RUN_STATUS='RUNNING' AND JOB_ID IN (SELECT DISTINCT JOB_RID FROM C_ODI_JOB_METADATA WHERE JOB_NAME LIKE '%ADJUSTER%' OR JOB_NAME LIKE '%NCM%' OR JOB_NAME LIKE '%PATIENT%') AND JOB_ID <> PNI_JOB_ID ;
      IF V_CHECK = 0 THEN*/
      FOR i IN
          (SELECT create_index FROM STG_MDM.MDM_USER_INDEXES a left join ALL_INDEXES b on a.index_name = b.index_name WHERE b.index_name is null and job_name=PVI_ODI_JOB_NAME)
      LOOP
      
           sql_stmt1:= i.create_index;
           dbms_output.put_line(sql_stmt1);            
            BEGIN
            EXECUTE IMMEDIATE sql_stmt1;
             EXCEPTION WHEN OTHERS THEN
             dbms_output.put_line('Exception while creating index - ' || sql_stmt1);
             END;
      END LOOP;
    UPDATE STG_MDM.R_ODI_JOB_RUN_DETAILS SET IS_INDEX_CREATED='Y' WHERE JOB_RUN_RID = PNI_SESS_NUMBER AND JOB_ID = PNI_JOB_ID;
    COMMIT;
   -- END IF;
END;

  PROCEDURE SP_ODI_DROP_INDEX
 (
    PVI_ODI_JOB_NAME IN VARCHAR2,
    PNI_SESS_NUMBER NUMBER )
    IS
    sql_stmt VARCHAR2(200);
     Begin
        for i in (select drop_index from  STG_MDM.MDM_USER_INDEXES a ,ALL_INDEXES b where a.index_name=b.index_name and a.job_name=PVI_ODI_JOB_NAME and b.TABLE_OWNER = 'STG_MDM')
        loop
            sql_stmt:= i.drop_index;
            dbms_output.put_line(sql_stmt);
            BEGIN
            EXECUTE IMMEDIATE sql_stmt;
             EXCEPTION WHEN OTHERS THEN
             dbms_output.put_line('Exception while dropping index - ' || sql_stmt);
             END;
        end loop;
        UPDATE STG_MDM.R_ODI_JOB_RUN_DETAILS SET IS_INDEX_CREATED='D' WHERE JOB_RUN_RID=PNI_SESS_NUMBER;
        COMMIT;
  End;

  PROCEDURE SP_SAVE_ODI_JOB_RUN_DETAILS
      (
        PNI_JOB_RUN_ID IN NUMBER
      , PVI_JOB_NAME IN VARCHAR2
      , PVI_STEP_FLAG IN VARCHAR2
      , PVI_LAST_RUN_TIME IN VARCHAR2
      , PVI_JOB_STATUS IN VARCHAR2
      , PVI_RUN_USER IN VARCHAR2
      ) AS
       V_JOB_ID      NUMBER;
       V_LAST_RUN_TIME  VARCHAR2(256);
       V_RUN_USER    R_ODI_JOB_RUN_DETAILS.CREATED_BY_ID%TYPE := '-1001';
  BEGIN
     SELECT JOB_RID
          INTO   V_JOB_ID
          FROM   C_ODI_JOB_METADATA
          WHERE  JOB_NAME = PVI_JOB_NAME;

          --V_LAST_RUN_TIME := PVI_LAST_RUN_TIME||TO_CHAR(CURRENT_TIMESTAMP,'AM');

    IF(PVI_STEP_FLAG = 'L') THEN
            INSERT INTO R_ODI_JOB_RUN_DETAILS
                (JOB_RUN_RID
                  ,JOB_ID
                  ,LAST_RUN_TIME
                  ,JOB_RUN_STATUS
                  ,CREATED_BY_ID
				  ,START_JOB_TIME
                  )
                  VALUES (  PNI_JOB_RUN_ID,
                            V_JOB_ID,
                            TO_TIMESTAMP(PVI_LAST_RUN_TIME,'YYYYMMDDHH24MISS'),
                            PVI_JOB_STATUS,
                            V_RUN_USER,
							CURRENT_TIMESTAMP
                          );
    END IF;
    ---------------------------------->> Rebuild Indexes on Staging Tables
        IF(PVI_JOB_NAME LIKE '%PATIENT%') THEN
          PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_PERSON');
          PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_PATIENT_CLAIM');
          PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_PERSON');
          PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_PATIENT_CLAIM');
          PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_ADDRESS');
          PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_ADDRESS');
        END IF;
        IF(PVI_JOB_NAME LIKE '%ADJUSTER%' OR PVI_JOB_NAME LIKE '%NCM%') THEN
          PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_PERSON');
          PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_ADJUSTER_BRANCH');
          PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_PERSON');
          PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_ADJUSTER_BRANCH');
          PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_ADDRESS');
          PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_ADDRESS');
        END IF;
        IF(PVI_JOB_NAME LIKE '%HQ%' OR PVI_JOB_NAME LIKE '%BRANCH%') THEN
         PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_BUSINESS');
         PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_BUSINESS');
         PKG_COMMON.SP_REBUILD_ALL_INDEXES ('STG_ADDRESS');
         PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_ADDRESS');
        END IF;


 END SP_SAVE_ODI_JOB_RUN_DETAILS;

   PROCEDURE SP_UPDATE_ODI_JOB_RUN_DETAILS
      (
        PNI_JOB_RUN_ID IN NUMBER
      , PVI_JOB_NAME IN VARCHAR2
      , PVI_LAST_RUN_TIME IN VARCHAR2
      , PVI_JOB_STATUS IN VARCHAR2
      , PVI_RUN_USER IN VARCHAR2
      , PVI_INSERT_COUNT IN NUMBER
      , PVI_UPDATE_COUNT IN NUMBER
      , PVI_END_JOB_TIME IN TIMESTAMP
      , PVI_OUT_INSERT_COUNT OUT NUMBER
      ) AS
       V_RUN_USER    R_ODI_JOB_RUN_DETAILS.CREATED_BY_ID%TYPE := '-1001';
       V_INSERTED    R_ODI_JOB_RUN_DETAILS.INSERTED_RECORDS%TYPE;
       V_UPDATED     R_ODI_JOB_RUN_DETAILS.UPDATED_RECORDS%TYPE;
  BEGIN

      PVI_OUT_INSERT_COUNT := PVI_INSERT_COUNT;
      SELECT INSERTED_RECORDS, UPDATED_RECORDS
      INTO V_INSERTED, V_UPDATED
      FROM R_ODI_JOB_RUN_DETAILS
      WHERE JOB_RUN_RID = PNI_JOB_RUN_ID;
      
      UPDATE R_ODI_JOB_RUN_DETAILS
      SET
        LAST_RUN_TIME = TO_TIMESTAMP(PVI_LAST_RUN_TIME,'YYYYMMDDHH24MISS')
        ,JOB_RUN_STATUS = PVI_JOB_STATUS
        ,LAST_MODIFIED_BY_ID = V_RUN_USER
        ,LAST_MODIFICATION_DATE = CURRENT_TIMESTAMP
        ,INSERTED_RECORDS = NVL(V_INSERTED,0) +PVI_INSERT_COUNT
        ,UPDATED_RECORDS = NVL(V_UPDATED,0) + PVI_UPDATE_COUNT
        ,END_JOB_TIME = PVI_END_JOB_TIME
      WHERE
        JOB_RUN_RID = PNI_JOB_RUN_ID;
 
 END SP_UPDATE_ODI_JOB_RUN_DETAILS;

  ----------------------------------->> To run SOAP Request
 PROCEDURE SP_ADDRESS_API_SOAP_REQUEST
  (PVI_SOURCE_REF IN VARCHAR2,
   PVI_SOURCE_NAME IN VARCHAR2,
   PVI_ENTITY_NAME IN VARCHAR2,
   PVI_ADDRESS IN VARCHAR2,
   PVI_CITY    IN VARCHAR2,
   PVI_STATE   IN VARCHAR2,
   PVI_ZIP     IN VARCHAR2,
   PVI_ADDRESS_TYPE IN VARCHAR2
  )IS
    soap_request  VARCHAR2(32767);
    soap_respond  CLOB;
    http_req      utl_http.req;
    http_resp     utl_http.resp;
    resp          XMLType;
    soap_err      exception;
    v_code        VARCHAR2(32767);
    v_msg         VARCHAR2(32767);
    v_len         number;
    l_buffer      Varchar2(32767);
    l_varchar2      VARCHAR2(32767);
    V_API_URL       VARCHAR2(32767);
    l_eof			      boolean;
    l_http_open_fl	boolean;
    l_sqlerrm       varchar2(32767);
    l_sqlcode         NUMBER;
    V_ADDRESS         VARCHAR2(256);
	V_CITY			  VARCHAR2(256);
  BEGIN
         BEGIN
              SELECT VALUE
              INTO V_API_URL
              FROM C_ODI_CONFIG
              WHERE PACKAGE_NAME = 'ADDRESS_API_URL';
              EXCEPTION WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20001,'ADDRESS API URL NOT FOUND ');
          END;
    -- Define the SOAP request according the the definition of the web service being called
    V_ADDRESS :=  REPLACE(PVI_ADDRESS,'&',ascii('&')||'AMPERSAND');
    DBMS_OUTPUT.PUT_LINE(V_ADDRESS);
    V_CITY :=  REPLACE(PVI_CITY,'&',ascii('&')||'AMPERSAND');
    DBMS_OUTPUT.PUT_LINE(V_CITY);
    soap_request:= '<?xml version = "1.0" encoding = "UTF-8"?>'||
							'<Envelope xmlns="http://schemas.xmlsoap.org/soap/envelope/">'||
									'<Body>'||
										'<Geocode xmlns="http://tempuri.org/">'||
											'<providerName>BingGeoProvider</providerName>'||
											'<providerVersion></providerVersion>'||
											'<!-- Optional -->'||
											'<addresses>'||
												'<!-- Optional -->'||
												'<BaseAddress xmlns="http://schemas.datacontract.org/2004/07/OCM.Enterprise.Geocoding.Services.BusinessEntities.DataContracts">'||
													'<Address>'||V_ADDRESS||'</Address>'||
													'<City>'||V_CITY||'</City>'||
													'<Country>[string?]</Country>'||
													'<County>[string?]</County>'||
													'<PlusFour>[string?]</PlusFour>'||
													'<SourceId>'||PVI_SOURCE_REF||'</SourceId>'||
													'<State>'||PVI_STATE||'</State>'||
													'<Zip>'||PVI_ZIP||'</Zip>'||
												'</BaseAddress>'||
											'</addresses>'||
                      '<consumerName>MDM</consumerName>'||
										'</Geocode>'||
									'</Body>'||
								'</Envelope>';
        l_http_open_fl:=FALSE;
        utl_http.set_body_charset('UTF-8');
        http_req:= utl_http.begin_request(V_API_URL, 'POST', 'HTTP/1.1');
        utl_http.set_header(http_req, 'Content-Type', 'text/xml');
        utl_http.set_header(http_req, 'SOAPAction', 'http://tempuri.org/IGeocodingService/Geocode');
        utl_http.set_header(http_req, 'Content-Length', lengthb(soap_request));
        utl_http.set_header(http_req, 'Download', ''); -- header requirements of particular web service
        utl_http.write_text(http_req, soap_request);
        l_http_open_fl := TRUE;
        http_resp:= utl_http.get_response(http_req);
        -- we receive the XML response from the web service  (saving it as a CLOB)
        dbms_lob.createtemporary( soap_respond, true );
        l_eof := false;
        LOOP
          EXIT WHEN l_eof;
            BEGIN
                utl_http.read_text( http_resp, l_buffer, 32767 );
                IF (l_buffer IS NOT NULL) AND LENGTH(l_buffer)>0 THEN
                        dbms_lob.writeAppend( soap_respond, LENGTH(l_buffer), l_buffer );
                END IF;
                EXCEPTION WHEN utl_http.end_of_body THEN
                l_eof := true;
            END;
        END LOOP;
        utl_http.end_response(http_resp);
        resp:= XMLType.createXML(soap_respond); -- Convert CLOB to
        l_varchar2 := resp.getStringVal();
        l_varchar2 := REPLACE(l_varchar2,'</GeocodeResponse></s:Body></s:Envelope>','');
        l_varchar2 := REPLACE(l_varchar2,'<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><GeocodeResponse xmlns="http://tempuri.org/">','');
        l_varchar2 := '<?xml version="1.0" ?>'||l_varchar2;
        l_varchar2 := REPLACE(l_varchar2,'a:','');
        resp := xmltype(l_varchar2);
        dbms_lob.freetemporary( soap_respond );
        INSERT INTO TMP_ADDRESS_API_OUTPUT(SOURCE_REF,SOURCE_NAME,ENTITY_NAME,RESPONSE_FILE_XML,CREATED_BY_ID,ADDRESS_TYPE)
          VALUES(PVI_SOURCE_REF,PVI_SOURCE_NAME,PVI_ENTITY_NAME,resp,'-1001',PVI_ADDRESS_TYPE);
         COMMIT;
        EXCEPTION WHEN OTHERS THEN
            l_sqlerrm := SQLERRM;
            l_SQLCODE := SQLCODE;
            IF l_http_open_fl THEN
                utl_http.end_response( http_resp );
            END IF;
            IF soap_respond IS NOT NULL THEN
                dbms_lob.freetemporary( soap_respond );
            END IF;
          RAISE;
  END SP_ADDRESS_API_SOAP_REQUEST;
  --------------------------------------------->> To Populate Address fields in staging
   PROCEDURE SP_UPDATE_STG_ADDRESS_DETAILS
  IS
    CURSOR C1 IS
    SELECT xml_data.*
    FROM
        (SELECT t.SOURCE_REF,t.source_name,t.entity_name,t.address_type, x.*,y.*,z.*
             FROM TMP_ADDRESS_API_OUTPUT t,
                  XMLTABLE ('/GeocodeResult/MatchedAddress/GeocodedAddress'
                            PASSING t.RESPONSE_FILE_XML
                            COLUMNS Address VARCHAR2(256) PATH '/GeocodedAddress/Address/text()',
                             City VARCHAR2(256) PATH '/GeocodedAddress/City/text()',
                             State VARCHAR2(30) PATH '/GeocodedAddress/State/text()',
                             zip VARCHAR2(11) PATH '/GeocodedAddress/Zip/text()'
                            ) x,
                    XMLTABLE ('/GeocodeResult/MatchedAddress/OriginalAddress'
                            PASSING t.RESPONSE_FILE_XML
                            COLUMNS OrigAddress VARCHAR2(256) PATH '/OriginalAddress/Address/text()',
                             OrigCity VARCHAR2(256) PATH '/OriginalAddress/City/text()',
                             OrigState VARCHAR2(30) PATH '/OriginalAddress/State/text()',
                             Origzip VARCHAR2(11) PATH '/OriginalAddress/Zip/text()'
                            )y,
                    XMLTABLE ('/GeocodeResult/MatchedAddress'
                            PASSING t.RESPONSE_FILE_XML
                            COLUMNS Status VARCHAR2(30) PATH '/MatchedAddress/Status/text()',
                             Flag VARCHAR2(30) PATH '/MatchedAddress/StatusFlag/text()'
                           )z
           )xml_data;
  BEGIN
        FOR F1 IN C1
        LOOP
        BEGIN

              -- If address has got updated, Then delete the ORG address
                DELETE FROM STG_ADDRESS
                  WHERE SOURCE_REF = F1.SOURCE_REF
                    AND SOURCE_NAME = F1.SOURCE_NAME
                      AND ENTITY_NAME = F1.ENTITY_NAME
                        AND ADDRESS_TYPE = DECODE(F1.ADDRESS_TYPE,'MAIN','ORG_MAIN','BILL','ORG_BILL','ALTERNATE','ORG_ALTERNATE','HOME','ORG_HOME');

                  UPDATE STG_ADDRESS
                  SET    ADDRESS_TYPE = DECODE(F1.ADDRESS_TYPE,'MAIN','ORG_MAIN','BILL','ORG_BILL','ALTERNATE','ORG_ALTERNATE','HOME','ORG_HOME'),
                         IS_ODI_PROCESSED = 'Y'
                  WHERE  SOURCE_REF = F1.SOURCE_REF
                  AND    SOURCE_NAME = F1.SOURCE_NAME
                  AND    ENTITY_NAME = F1.ENTITY_NAME
                  AND    ADDRESS_TYPE = DECODE(F1.ADDRESS_TYPE,'MAIN','MAIN','BILL','BILL','ALTERNATE','ALTERNATE','HOME','HOME');

                  IF(UPPER(F1.FLAG) = 'FALSE' OR LENGTH(F1.STATE) > 2 OR LENGTH(F1.ZIP) > 11 OR LENGTH(F1.ADDRESS) > 256 OR LENGTH(F1.CITY) > 256 ) THEN
                        INSERT INTO STG_ADDRESS(SOURCE_REF,SOURCE_NAME,ENTITY_NAME,ADDRESS_TYPE,
                                                  ADDRESS1,ADDRESS2,ADDRESS3,CITY,STATE_CODE,ZIP_PLUS_FOUR,
                                                    IS_ADDRESS_VERIFIED,IS_ODI_PROCESSED,CREATION_TS,
                                                      CREATED_BY_ID,LAST_MODIFIED_BY_ID,LAST_MODIFICATION_TS
                                                 ) VALUES
                                                 (F1.SOURCE_REF,F1.SOURCE_NAME,F1.ENTITY_NAME,DECODE(F1.ADDRESS_TYPE,'MAIN','MAIN','BILL','BILL','ALTERNATE','ALTERNATE','HOME','HOME'),
                                                    UPPER(F1.ORIGADDRESS),NULL,NULL,UPPER(F1.ORIGCITY),UPPER(F1.ORIGSTATE),UPPER(F1.ORIGZIP),
                                                     'N','Y',CURRENT_TIMESTAMP,
                                                      '-1001',NULL,NULL
                                                 );
                  ELSIF(UPPER(F1.FLAG) = 'TRUE' AND ((F1.STATE IS NULL AND F1.ORIGSTATE IS NOT NULL) OR (F1.ZIP IS NULL AND F1.ORIGZIP IS NOT NULL)  OR (F1.ADDRESS IS NULL AND F1.ORIGADDRESS IS NOT NULL) OR (F1.CITY IS NULL AND F1.ORIGCITY IS NOT NULL))) THEN
                        INSERT INTO STG_ADDRESS(SOURCE_REF,SOURCE_NAME,ENTITY_NAME,ADDRESS_TYPE,
                                                  ADDRESS1,ADDRESS2,ADDRESS3,CITY,STATE_CODE,ZIP_PLUS_FOUR,
                                                    IS_ADDRESS_VERIFIED,IS_ODI_PROCESSED,CREATION_TS,
                                                      CREATED_BY_ID,LAST_MODIFIED_BY_ID,LAST_MODIFICATION_TS
                                                 ) VALUES
                                                 (F1.SOURCE_REF,F1.SOURCE_NAME,F1.ENTITY_NAME,DECODE(F1.ADDRESS_TYPE,'MAIN','MAIN','BILL','BILL','ALTERNATE','ALTERNATE','HOME','HOME'),
                                                    UPPER(F1.ORIGADDRESS),NULL,NULL,UPPER(F1.ORIGCITY),UPPER(F1.ORIGSTATE),UPPER(F1.ORIGZIP),
                                                     'N','Y',CURRENT_TIMESTAMP,
                                                      '-1001',NULL,NULL
                                                 );
                  ELSE
                        INSERT INTO STG_ADDRESS(SOURCE_REF,SOURCE_NAME,ENTITY_NAME,ADDRESS_TYPE,
                                                  ADDRESS1,ADDRESS2,ADDRESS3,CITY,STATE_CODE,ZIP_PLUS_FOUR,
                                                    IS_ADDRESS_VERIFIED,IS_ODI_PROCESSED,CREATION_TS,
                                                      CREATED_BY_ID,LAST_MODIFIED_BY_ID,LAST_MODIFICATION_TS
                                                 ) VALUES
                                                 (F1.SOURCE_REF,F1.SOURCE_NAME,F1.ENTITY_NAME,DECODE(F1.ADDRESS_TYPE,'MAIN','MAIN','BILL','BILL','ALTERNATE','ALTERNATE','HOME','HOME'),
                                                    UPPER(REPLACE(F1.ADDRESS,'38AMPERSAND','&')),NULL,NULL,UPPER(REPLACE(F1.CITY,'38AMPERSAND','&')),UPPER(F1.STATE),UPPER(F1.ZIP),
                                                     'Y','Y',CURRENT_TIMESTAMP,
                                                      '-1001',NULL,NULL
                                                 );
                  END IF;
                  EXCEPTION WHEN OTHERS THEN
                    PKG_COMMON.SP_ERROR_HANDLER(F1.SOURCE_REF,F1.SOURCE_NAME,F1.ENTITY_NAME);
        END;
        COMMIT;
        END LOOP;
        EXCEPTION WHEN OTHERS THEN
             PKG_COMMON.SP_ERROR_HANDLER(NULL,NULL,NULL);
  END SP_UPDATE_STG_ADDRESS_DETAILS;
  --------------------------------------------------->> Wrapper SP (To save address api data)
    PROCEDURE SP_SAVE_ADDRESS_API
  ( PVI_ODI_PACKAGE_NAME IN VARCHAR2
  ) AS
    V_SOURCE_NAME     STG_BUSINESS.SOURCE_NAME%TYPE;
    V_ENTITY_NAME     STG_BUSINESS.ENTITY_NAME%TYPE;
    V_QUERY           VARCHAR2(2000);
    V_QUERY1           VARCHAR2(2000);
    V_QUERY2           VARCHAR2(2000);
    V_QUERY3           VARCHAR2(2000);
    LV_SOURCE_REF     STG_BUSINESS.SOURCE_REF%TYPE;
    LV_SOURCE_NAME    STG_BUSINESS.SOURCE_NAME%TYPE;
    LV_ENTITY_NAME    STG_BUSINESS.ENTITY_NAME%TYPE;
    LV_ADDRESS        VARCHAR2(500);
    LV_CITY           STG_ADDRESS.CITY%TYPE;
    LV_STATE_CODE     STG_ADDRESS.STATE_CODE%TYPE;
    LV_ZIP            STG_ADDRESS.ZIP_PLUS_FOUR%TYPE;
    C1                SYS_REFCURSOR;
    C2                SYS_REFCURSOR;
    C3                SYS_REFCURSOR;
    C4                SYS_REFCURSOR;
    V_PO_FLAG         CHAR(10);
    L_POBOX           VARCHAR2(256);
    V_API_COUNT       NUMBER;

  BEGIN
        EXECUTE IMMEDIATE ('TRUNCATE TABLE TMP_ADDRESS_API_OUTPUT');
        V_SOURCE_NAME := SUBSTR(PVI_ODI_PACKAGE_NAME,1,2);
        V_ENTITY_NAME := Regexp_Substr(PVI_ODI_PACKAGE_NAME,'[^_]+',1,2);

        IF (V_SOURCE_NAME = 'DX') THEN
            V_SOURCE_NAME := V_SOURCE_NAME||'.PHOENIX';
        ELSIF (V_SOURCE_NAME = 'TL') THEN
            V_SOURCE_NAME := V_SOURCE_NAME||'.DYNAMITE';
        ELSIF (V_SOURCE_NAME = 'TN') THEN
            V_SOURCE_NAME :='TNL.STOPS';
        ELSIF (V_SOURCE_NAME = 'EB' OR V_SOURCE_NAME = 'CS') THEN
            V_SOURCE_NAME := 'HHEDM.CSRD';
        ELSIF (V_SOURCE_NAME = 'DD') THEN
            V_SOURCE_NAME := V_SOURCE_NAME||'.SMILE';
        ELSIF (V_SOURCE_NAME = 'PT') THEN
          V_SOURCE_NAME := V_SOURCE_NAME||'.COMPASS';
        ELSIF (V_SOURCE_NAME = 'TH') THEN
          V_SOURCE_NAME := 'TECHEALTH';
        END IF;

        IF(V_ENTITY_NAME IN ('HQ','_H')) THEN
            V_ENTITY_NAME := 'HQ';
        ELSIF(V_ENTITY_NAME IN('BRANCH')) THEN
            V_ENTITY_NAME := 'BRANCH';
        ELSIF(V_ENTITY_NAME IN('AD','_A')) THEN
            V_ENTITY_NAME := 'ADJUSTER';
        ELSIF(V_ENTITY_NAME IN('PA','_P')) THEN
            V_ENTITY_NAME := 'PATIENT';
        END IF;

        ---- To check if address API is active for the job or not
        BEGIN
              SELECT COUNT(1)
              INTO   V_API_COUNT
              FROM   C_ODI_JOB_METADATA
              WHERE JOB_NAME = PVI_ODI_PACKAGE_NAME
              AND   USE_ADDRESS_API = 'Y';
              EXCEPTION WHEN OTHERS THEN
                  RAISE_APPLICATION_ERROR(-20001,'JOB NOT FOUND IN C_ODI_JOB_METADATA TABLE');
        END;


            IF(V_API_COUNT = 0) THEN
                GOTO END_OF_FILE;
            END IF;

        --- Stats gathering on STG_ADDRESS table
           --PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('STG_ADDRESS');
           V_QUERY := 'SELECT TRIM(source_ref),
                           TRIM(source_name),
                           TRIM(entity_name),
                           TRIM(REGEXP_REPLACE(ADDRESS1 || '' '' || ADDRESS2 || '' '' || ADDRESS3,''[[:space:]]+''||'',''||CHR(32))) AS ADDRESS,
                           TRIM(city) as city,
                           TRIM(state_code) as state_code,
                           TRIM(zip_plus_four) as zip
                    FROM STG_ADDRESS
                    WHERE source_name = '||''''||V_SOURCE_NAME||''''||
                     ' and ENTITY_NAME = '||''''||V_ENTITY_NAME||''''||
                     ' and ADDRESS_TYPE = '||'''MAIN'''||
                     ' and IS_ADDRESS_VERIFIED = '||'''Z'''||
                     ' and (TRIM(ADDRESS1) IS NOT NULL'||
                     ' or  TRIM(ADDRESS2) IS NOT NULL'||
                     ' or  TRIM(ADDRESS3) IS NOT NULL'||
                     ' or  TRIM(CITY)     IS NOT NULL'||
                     ' or  TRIM(STATE_CODE) IS NOT NULL'||
                     ' or  TRIM(ZIP_PLUS_FOUR) IS NOT NULL)';



            IF(V_ENTITY_NAME IN('BRANCH')) THEN
               V_QUERY1 := 'SELECT TRIM(source_ref),
                           TRIM(source_name),
                           TRIM(entity_name),
                           TRIM(REGEXP_REPLACE(ADDRESS1 || '' '' || ADDRESS2 || '' '' || ADDRESS3,''[[:space:]]+''||'',''||CHR(32))) AS ADDRESS,
                           TRIM(city) as city,
                           TRIM(state_code) as state_code,
                           TRIM(zip_plus_four) as zip
                    FROM STG_ADDRESS
                    WHERE source_name = '||''''||V_SOURCE_NAME||''''||
                     ' and ENTITY_NAME = '||''''||V_ENTITY_NAME||''''||
                     ' and ADDRESS_TYPE = '||'''BILL'''||
                     ' and IS_ADDRESS_VERIFIED = '||'''Z'''||
                     ' and (TRIM(ADDRESS1) IS NOT NULL'||
                     ' or  TRIM(ADDRESS2) IS NOT NULL'||
                     ' or  TRIM(ADDRESS3) IS NOT NULL'||
                     ' or  TRIM(CITY)     IS NOT NULL'||
                     ' or  TRIM(STATE_CODE) IS NOT NULL'||
                     ' or  TRIM(ZIP_PLUS_FOUR) IS NOT NULL)';

            END IF;
            IF (V_ENTITY_NAME IN('PATIENT')) THEN

                V_QUERY2 := 'SELECT TRIM(source_ref),
                           TRIM(source_name),
                           TRIM(entity_name),
                           TRIM(REGEXP_REPLACE(ADDRESS1 || '' '' || ADDRESS2 || '' '' || ADDRESS3,''[[:space:]]+''||'',''||CHR(32))) AS ADDRESS,
                           TRIM(city) as city,
                           TRIM(state_code) as state_code,
                           TRIM(zip_plus_four) as zip
                    FROM STG_ADDRESS
                    WHERE source_name = '||''''||V_SOURCE_NAME||''''||
                     ' and ENTITY_NAME = '||''''||V_ENTITY_NAME||''''||
                     ' and ADDRESS_TYPE = '||'''HOME'''||
                     ' and IS_ADDRESS_VERIFIED = '||'''Z'''||
                     ' and (TRIM(ADDRESS1) IS NOT NULL'||
                     ' or  TRIM(ADDRESS2) IS NOT NULL'||
                     ' or  TRIM(ADDRESS3) IS NOT NULL'||
                     ' or  TRIM(CITY)     IS NOT NULL'||
                     ' or  TRIM(STATE_CODE) IS NOT NULL'||
                     ' or  TRIM(ZIP_PLUS_FOUR) IS NOT NULL)';
                V_QUERY3 := 'SELECT TRIM(source_ref),
                           TRIM(source_name),
                           TRIM(entity_name),
                           TRIM(REGEXP_REPLACE(ADDRESS1 || '' '' || ADDRESS2 || '' '' || ADDRESS3,''[[:space:]]+''||'',''||CHR(32))) AS ADDRESS,
                           TRIM(city) as city,
                           TRIM(state_code) as state_code,
                           TRIM(zip_plus_four) as zip
                    FROM STG_ADDRESS
                    WHERE source_name = '||''''||V_SOURCE_NAME||''''||
                     ' and ENTITY_NAME = '||''''||V_ENTITY_NAME||''''||
                     ' and ADDRESS_TYPE = '||'''ALTERNATE'''||
                     ' and IS_ADDRESS_VERIFIED = '||'''Z'''||
                     ' and (TRIM(ADDRESS1) IS NOT NULL'||
                     ' or  TRIM(ADDRESS2) IS NOT NULL'||
                     ' or  TRIM(ADDRESS3) IS NOT NULL'||
                     ' or  TRIM(CITY)     IS NOT NULL'||
                     ' or  TRIM(STATE_CODE) IS NOT NULL'||
                     ' or  TRIM(ZIP_PLUS_FOUR) IS NOT NULL)';
              END IF;
          OPEN C1 FOR V_QUERY;
          LOOP
          BEGIN


              FETCH C1 INTO LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME,LV_ADDRESS,LV_CITY,LV_STATE_CODE,LV_ZIP;
              EXIT WHEN C1%NOTFOUND;

              -- Do not hit address API where Address is POBOX
              IF(((REPLACE(REPLACE(REPLACE(REPLACE(NVL(UPPER(LV_ADDRESS),'000'),' ',''),'.',''),'-',''),';','')) LIKE '%POBOX%')) THEN

                          UPDATE STG_ADDRESS
                          SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                          WHERE SOURCE_REF = LV_SOURCE_REF
                          AND   SOURCE_NAME = LV_SOURCE_NAME
                          AND   ENTITY_NAME = LV_ENTITY_NAME
                          AND   ADDRESS_TYPE = 'MAIN';
                -- If any of the address component is null , mark it as Unavailable
              ELSIF (LV_ADDRESS IS NULL OR LV_CITY IS NULL OR LV_STATE_CODE IS NULL) THEN

                          UPDATE STG_ADDRESS
                          SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                          WHERE SOURCE_REF = LV_SOURCE_REF
                          AND   SOURCE_NAME = LV_SOURCE_NAME
                          AND   ENTITY_NAME = LV_ENTITY_NAME
                          AND   ADDRESS_TYPE = 'MAIN';
              ELSE

                    SP_ADDRESS_API_SOAP_REQUEST
                    (
                      PVI_SOURCE_REF => LV_SOURCE_REF,
                      PVI_SOURCE_NAME => LV_SOURCE_NAME,
                      PVI_ENTITY_NAME => LV_ENTITY_NAME,
                      PVI_ADDRESS => LV_ADDRESS,
                      PVI_CITY => LV_CITY,
                      PVI_STATE => LV_STATE_CODE,
                      PVI_ZIP => LV_ZIP,
                      PVI_ADDRESS_TYPE => 'MAIN'
                    );
              END IF;
              COMMIT;
                  LV_SOURCE_REF := NULL;
                  LV_SOURCE_NAME := NULL;
                  LV_ENTITY_NAME := NULL;
                  LV_ADDRESS    := NULL;
                  LV_CITY       := NULL;
                  LV_STATE_CODE := NULL;
                  LV_ZIP        := NULL;
            EXCEPTION WHEN OTHERS THEN
              PKG_COMMON.SP_ERROR_HANDLER(LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME);
        END;
        END LOOP;
        CLOSE C1;
        IF(V_ENTITY_NAME IN('BRANCH')) THEN
              OPEN C2 FOR V_QUERY1;
              LOOP
              BEGIN
                  FETCH C2 INTO LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME,LV_ADDRESS,LV_CITY,LV_STATE_CODE,LV_ZIP;
                  EXIT WHEN C2%NOTFOUND;
                  /* Do not hit address API where Address is POBOX */
                  IF(((REPLACE(REPLACE(REPLACE(REPLACE(NVL(UPPER(LV_ADDRESS),'000'),' ',''),'.',''),'-',''),';','')) LIKE '%POBOX%')) THEN
                        UPDATE STG_ADDRESS
                        SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                        WHERE SOURCE_REF = LV_SOURCE_REF
                        AND   SOURCE_NAME = LV_SOURCE_NAME
                        AND   ENTITY_NAME = LV_ENTITY_NAME
                        AND   ADDRESS_TYPE = 'BILL';
                     /* If any of the address component is null , mark it as Unavailable */
                   ELSIF (LV_ADDRESS IS NULL OR LV_CITY IS NULL OR LV_STATE_CODE IS NULL) THEN
                        UPDATE STG_ADDRESS
                        SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                        WHERE SOURCE_REF = LV_SOURCE_REF
                        AND   SOURCE_NAME = LV_SOURCE_NAME
                        AND   ENTITY_NAME = LV_ENTITY_NAME
                        AND   ADDRESS_TYPE = 'BILL';
                   ELSE
                      SP_ADDRESS_API_SOAP_REQUEST
                      (
                        PVI_SOURCE_REF => LV_SOURCE_REF,
                        PVI_SOURCE_NAME => LV_SOURCE_NAME,
                        PVI_ENTITY_NAME => LV_ENTITY_NAME,
                        PVI_ADDRESS => LV_ADDRESS,
                        PVI_CITY => LV_CITY,
                        PVI_STATE => LV_STATE_CODE,
                        PVI_ZIP => LV_ZIP,
                        PVI_ADDRESS_TYPE => 'BILL'
                      );
                   END IF;
                  COMMIT;
                        LV_SOURCE_REF := NULL;
                        LV_SOURCE_NAME := NULL;
                        LV_ENTITY_NAME := NULL;
                        LV_ADDRESS    := NULL;
                        LV_CITY       := NULL;
                        LV_STATE_CODE := NULL;
                        LV_ZIP        := NULL;
                  EXCEPTION WHEN OTHERS THEN
                    PKG_COMMON.SP_ERROR_HANDLER(LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME);
            END;
            END LOOP;
            CLOSE C2;
        END IF;
        IF(V_ENTITY_NAME='PATIENT') THEN
              OPEN C3 FOR V_QUERY2;
              LOOP
              BEGIN

                  FETCH C3 INTO LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME,LV_ADDRESS,LV_CITY,LV_STATE_CODE,LV_ZIP;
                  EXIT WHEN C3%NOTFOUND;


              -- Do not hit address API where Address is POBOX
                  IF(((REPLACE(REPLACE(REPLACE(REPLACE(NVL(UPPER(LV_ADDRESS),'000'),' ',''),'.',''),'-',''),';','')) LIKE '%POBOX%')) THEN

                            UPDATE STG_ADDRESS
                            SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                            WHERE SOURCE_REF = LV_SOURCE_REF
                            AND   SOURCE_NAME = LV_SOURCE_NAME
                            AND   ENTITY_NAME = LV_ENTITY_NAME
                            AND   ADDRESS_TYPE = 'HOME';
                -- If any of the address component is null , mark it as Unavailable
                  ELSIF (LV_ADDRESS IS NULL OR LV_CITY IS NULL OR LV_STATE_CODE IS NULL) THEN

                            UPDATE STG_ADDRESS
                            SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                            WHERE SOURCE_REF = LV_SOURCE_REF
                            AND   SOURCE_NAME = LV_SOURCE_NAME
                            AND   ENTITY_NAME = LV_ENTITY_NAME
                            AND   ADDRESS_TYPE = 'HOME';
                  ELSE

                      SP_ADDRESS_API_SOAP_REQUEST
                      (
                        PVI_SOURCE_REF => LV_SOURCE_REF,
                        PVI_SOURCE_NAME => LV_SOURCE_NAME,
                        PVI_ENTITY_NAME => LV_ENTITY_NAME,
                        PVI_ADDRESS => LV_ADDRESS,
                        PVI_CITY => LV_CITY,
                        PVI_STATE => LV_STATE_CODE,
                        PVI_ZIP => LV_ZIP,
                        PVI_ADDRESS_TYPE => 'HOME'
                      );
                END IF;
                COMMIT;
                    LV_SOURCE_REF := NULL;
                    LV_SOURCE_NAME := NULL;
                    LV_ENTITY_NAME := NULL;
                    LV_ADDRESS    := NULL;
                    LV_CITY       := NULL;
                    LV_STATE_CODE := NULL;
                    LV_ZIP        := NULL;
                EXCEPTION WHEN OTHERS THEN
                PKG_COMMON.SP_ERROR_HANDLER(LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME);
              END;
              END LOOP;
              CLOSE C3;
              OPEN C4 FOR V_QUERY3;
              LOOP
              BEGIN

                  FETCH C4 INTO LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME,LV_ADDRESS,LV_CITY,LV_STATE_CODE,LV_ZIP;
                  EXIT WHEN C4%NOTFOUND;

              -- Do not hit address API where Address is POBOX
                  IF(((REPLACE(REPLACE(REPLACE(REPLACE(NVL(UPPER(LV_ADDRESS),'000'),' ',''),'.',''),'-',''),';','')) LIKE '%POBOX%')) THEN

                            UPDATE STG_ADDRESS
                            SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                            WHERE SOURCE_REF = LV_SOURCE_REF
                            AND   SOURCE_NAME = LV_SOURCE_NAME
                            AND   ENTITY_NAME = LV_ENTITY_NAME
                            AND   ADDRESS_TYPE = 'ALTERNATE';
                -- If any of the address component is null , mark it as Unavailable
                  ELSIF (LV_ADDRESS IS NULL OR LV_CITY IS NULL OR LV_STATE_CODE IS NULL) THEN

                            UPDATE STG_ADDRESS
                            SET   IS_ADDRESS_VERIFIED = 'U',IS_ODI_PROCESSED='Y'
                            WHERE SOURCE_REF = LV_SOURCE_REF
                            AND   SOURCE_NAME = LV_SOURCE_NAME
                            AND   ENTITY_NAME = LV_ENTITY_NAME
                            AND   ADDRESS_TYPE = 'ALTERNATE';
                  ELSE

                      SP_ADDRESS_API_SOAP_REQUEST
                      (
                        PVI_SOURCE_REF => LV_SOURCE_REF,
                        PVI_SOURCE_NAME => LV_SOURCE_NAME,
                        PVI_ENTITY_NAME => LV_ENTITY_NAME,
                        PVI_ADDRESS => LV_ADDRESS,
                        PVI_CITY => LV_CITY,
                        PVI_STATE => LV_STATE_CODE,
                        PVI_ZIP => LV_ZIP,
                        PVI_ADDRESS_TYPE => 'WORK'
                      );
                END IF;
                COMMIT;
                    LV_SOURCE_REF := NULL;
                    LV_SOURCE_NAME := NULL;
                    LV_ENTITY_NAME := NULL;
                    LV_ADDRESS    := NULL;
                    LV_CITY       := NULL;
                    LV_STATE_CODE := NULL;
                    LV_ZIP        := NULL;
                EXCEPTION WHEN OTHERS THEN
                PKG_COMMON.SP_ERROR_HANDLER(LV_SOURCE_REF,LV_SOURCE_NAME,LV_ENTITY_NAME);
              END;
              END LOOP;
              CLOSE C4;
        END IF;
                  /*
                      Call SP_UPDATE_STG_ADDRESS_DETAILS to process the records from TMP_ADDRESS_API_OUTPUT table
                  */
                   PKG_COMMON.SP_GATHER_STATS_ALL_TABLES ('TMP_ADDRESS_API_OUTPUT');

                   SP_UPDATE_STG_ADDRESS_DETAILS;
                      V_SOURCE_NAME := NULL;
                      V_ENTITY_NAME := NULL;
                      V_QUERY       := NULL;
                      V_QUERY1      := NULL;

     <<END_OF_FILE>>


            UPDATE STG_ADDRESS
            SET  IS_ADDRESS_VERIFIED = 'N',
                 IS_ODI_PROCESSED = 'Y'
            WHERE SOURCE_NAME = V_SOURCE_NAME
            AND ENTITY_NAME = V_ENTITY_NAME AND V_ENTITY_NAME != 'PATIENT' ;
            COMMIT;

             EXCEPTION WHEN OTHERS THEN
               PKG_COMMON.SP_ERROR_HANDLER(NULL,V_SOURCE_NAME,V_ENTITY_NAME);
  END SP_SAVE_ADDRESS_API;
/*
  -- Call Service Package AGGSVC.PKG_SERVICE_DATA
*/
     /*PROCEDURE SP_SAVE_SERVICE_DATA
     (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
      ) IS
      BEGIN
              IF(PVI_ODI_PACKAGE_NAME LIKE 'DX%') THEN
                PKG_COMMON.SP_GATHER_STATS_ALL_TABLES('STG_DX_SERVICE_DATA');
                PKG_SERVICE_DATA.SP_SERVICE_DATA_LOAD ('STG_DX_SERVICE_DATA');
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'TL%') THEN
                PKG_COMMON.SP_GATHER_STATS_ALL_TABLES('STG_TL_SERVICE_DATA');
                PKG_SERVICE_DATA.SP_SERVICE_DATA_LOAD ('STG_TL_SERVICE_DATA');
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'DD%') THEN
                PKG_COMMON.SP_GATHER_STATS_ALL_TABLES('STG_DD_SERVICE_DATA');
               PKG_SERVICE_DATA.SP_SERVICE_DATA_LOAD ('STG_DD_SERVICE_DATA');
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'CSRD%') THEN
                PKG_COMMON.SP_GATHER_STATS_ALL_TABLES('STG_EBS_SERVICE_DATA');
                PKG_SERVICE_DATA.SP_SERVICE_DATA_LOAD ('STG_EBS_SERVICE_DATA');
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'PT%') THEN
                PKG_COMMON.SP_GATHER_STATS_ALL_TABLES('STG_PT_SERVICE_DATA');
                PKG_SERVICE_DATA.SP_SERVICE_DATA_LOAD ('STG_PT_SERVICE_DATA');
              ELSE
                      NULL;
              END IF;
              ------------------------------->> Update Claim Group ID
              PKG_SERVICE_DATA.SP_UPDATE_CLAIM_GROUP_ID;
      END;*/
    /*
        Service Data PreLoad Setup
    */
     /*PROCEDURE SP_SERVICE_DATA_PRELOAD
     (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
     ) IS
     BEGIN
              IF(PVI_ODI_PACKAGE_NAME LIKE 'DX%') THEN
                  DELETE
                    FROM STG_DX_SERVICE_DATA
                    WHERE IS_RECORD_PROCESSED IN ('Y','P');
                    COMMIT;
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'TL%') THEN
                   DELETE
                    FROM STG_TL_SERVICE_DATA
                    WHERE IS_RECORD_PROCESSED IN ('Y','P');
                    COMMIT;
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'DD%') THEN
                   DELETE
                    FROM STG_DD_SERVICE_DATA
                    WHERE IS_RECORD_PROCESSED IN ('Y','P');
                    COMMIT;
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'CSRD%') THEN
                   DELETE
                    FROM STG_EBS_SERVICE_DATA
                    WHERE IS_RECORD_PROCESSED IN ('Y','P');
                    COMMIT;
              ELSIF(PVI_ODI_PACKAGE_NAME LIKE 'PT%') THEN
                   DELETE
                    FROM STG_PT_SERVICE_DATA
                    WHERE IS_RECORD_PROCESSED IN ('Y','P');
                    COMMIT;
              ELSE
                      NULL;
              END IF;
     END;*/
    /*
             ODI Post Load Setup
    */
     PROCEDURE SP_MDM_DATA_POSTLOAD
    (
        PVI_ODI_PACKAGE_NAME IN VARCHAR2
    ) IS
      V_SOURCE_NAME     STG_BUSINESS.SOURCE_NAME%TYPE;
      V_ENTITY_NAME     STG_BUSINESS.ENTITY_NAME%TYPE;
    BEGIN
          V_SOURCE_NAME := SUBSTR(PVI_ODI_PACKAGE_NAME,1,2);
          V_ENTITY_NAME := Regexp_Substr(PVI_ODI_PACKAGE_NAME,'[^_]+',1,2);

          IF (V_SOURCE_NAME = 'DX') THEN
               V_SOURCE_NAME := V_SOURCE_NAME||'.PHOENIX';
          ELSIF (V_SOURCE_NAME = 'TL') THEN
                V_SOURCE_NAME := V_SOURCE_NAME||'.DYNAMITE';
          ELSIF (V_SOURCE_NAME = 'TN') THEN


                V_SOURCE_NAME := 'TNL.STOPS';
          ELSIF (V_SOURCE_NAME = 'EB' OR V_SOURCE_NAME = 'CS') THEN
               V_SOURCE_NAME := 'HHEDM.CSRD';
          ELSIF (V_SOURCE_NAME = 'DD') THEN
              V_SOURCE_NAME := V_SOURCE_NAME||'.SMILE';
          ELSIF (V_SOURCE_NAME = 'PT') THEN
            V_SOURCE_NAME := V_SOURCE_NAME||'.COMPASS';


          ELSIF (V_SOURCE_NAME = 'TH') THEN
            V_SOURCE_NAME := 'TECHEALTH';
          END IF;
          IF(V_ENTITY_NAME IN ('HQ','_H')) THEN
              V_ENTITY_NAME := 'HQ';

          ELSIF(V_ENTITY_NAME IN('BRANCH')) THEN
              V_ENTITY_NAME := 'BRANCH';
		 ELSIF(V_ENTITY_NAME IN('AD','_A')) THEN
              V_ENTITY_NAME := 'ADJUSTER';
          ELSIF(V_ENTITY_NAME IN('PA','_P')) THEN
              V_ENTITY_NAME := 'PATIENT';
          END IF;


           IF(V_ENTITY_NAME IN ('HQ','BRANCH')) THEN
              UPDATE STG_BUSINESS
              SET    IS_ODI_PROCESSED = 'Y',
                     CREATION_TS = CURRENT_TIMESTAMP,
                     CREATED_BY_ID = '-1001'
              WHERE  IS_ODI_PROCESSED = 'N'
              AND      SOURCE_NAME = V_SOURCE_NAME
              AND      ENTITY_NAME = V_ENTITY_NAME

              AND      NVL(ERR_FLAG,1)!= 'Y';

          ELSIF (V_ENTITY_NAME IN ('ADJUSTER','NCM')) THEN
                UPDATE STG_PERSON
                SET    IS_ODI_PROCESSED = 'Y',
                       CREATION_TS = CURRENT_TIMESTAMP,
                       CREATED_BY_ID = '-1001'
                WHERE IS_ODI_PROCESSED = 'N'
                 AND  SOURCE_NAME = V_SOURCE_NAME
                 AND  ENTITY_NAME = V_ENTITY_NAME

				 AND      NVL(ERR_FLAG,1)!= 'Y';

                UPDATE STG_ADJUSTER_BRANCH
                SET    IS_ODI_PROCESSED = 'Y'
               WHERE    IS_ODI_PROCESSED = 'N'
               AND      SOURCE_NAME = V_SOURCE_NAME
               AND      ENTITY_NAME = V_ENTITY_NAME;
          ELSIF (V_ENTITY_NAME IN ('PATIENT')) THEN
            UPDATE STG_PERSON
                SET ERR_FLAG ='N'
                WHERE ERR_FLAG = 'C';

                UPDATE STG_PERSON
                SET    IS_ODI_PROCESSED = 'Y',
                       CREATION_TS = CURRENT_TIMESTAMP,
                       CREATED_BY_ID = '-1001'
                WHERE    IS_ODI_PROCESSED = 'N'
                AND      SOURCE_NAME = V_SOURCE_NAME
                AND      ENTITY_NAME = V_ENTITY_NAME
                AND      NVL(ERR_FLAG,1)!= 'Y';

                UPDATE STG_PATIENT_CLAIM
                SET    IS_ODI_PROCESSED = 'Y'
                WHERE    IS_ODI_PROCESSED = 'N'
                AND      SOURCE_NAME = V_SOURCE_NAME
                AND      ENTITY_NAME = V_ENTITY_NAME;


                UPDATE STG_ADDRESS
                SET    IS_ODI_PROCESSED = 'Y'
                WHERE    IS_ODI_PROCESSED = 'N'
                AND      SOURCE_NAME = V_SOURCE_NAME
                AND      ENTITY_NAME = V_ENTITY_NAME;-- AND IS_ADDRESS_VERIFIED IS NULL;  

          END IF;

          COMMIT;
          EXCEPTION WHEN OTHERS THEN
            PKG_COMMON.SP_ERROR_HANDLER(NULL,NULL,NULL);
            ROLLBACK;
    END;

    /*The logic for Stripping the company name*/
   PROCEDURE SP_RM_STR_BRANCH_COMP_NAME
    (
    PVI_ODI_PACKAGE_NAME IN VARCHAR2
    )
    IS
      V_SOURCE_NAME     STG_BUSINESS.SOURCE_NAME%TYPE;
      V_ENTITY_NAME     STG_BUSINESS.ENTITY_NAME%TYPE;
    BEGIN
     V_SOURCE_NAME := SUBSTR(PVI_ODI_PACKAGE_NAME,1,2);
     V_ENTITY_NAME := Regexp_Substr(PVI_ODI_PACKAGE_NAME,'[^_]+',1,2);
      IF (V_SOURCE_NAME = 'DX') THEN
               V_SOURCE_NAME := V_SOURCE_NAME||'.PHOENIX';
          ELSIF (V_SOURCE_NAME = 'TL') THEN
                V_SOURCE_NAME := V_SOURCE_NAME||'.DYNAMITE';
          ELSIF (V_SOURCE_NAME = 'EB' OR V_SOURCE_NAME = 'CS') THEN
               V_SOURCE_NAME := 'HHEDM.CSRD';
          ELSIF (V_SOURCE_NAME = 'DD') THEN
              V_SOURCE_NAME := V_SOURCE_NAME||'.SMILE';
          ELSIF (V_SOURCE_NAME = 'PT') THEN
            V_SOURCE_NAME := V_SOURCE_NAME||'.COMPASS';
          ELSIF (V_SOURCE_NAME = 'TH') THEN
            V_SOURCE_NAME := 'TECHEALTH';
      END IF;

   IF(V_ENTITY_NAME = 'BRANCH') THEN




            EXECUTE IMMEDIATE ('ALTER INDEX XIE2_COMPANY_NAME REBUILD');


            PKG_COMMON.SP_GATHER_STATS_ALL_TABLES (  PVI_TABLE_NAME => 'STG_BUSINESS') ;


            INSERT INTO GTT_CLEANUP_MATCH_DATA
            SELECT   /*+ index(b,XIE2_COMPANY_NAME) */
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.COMPANY_NAME,

                                    a.MATCH_VALUE,
                                    b.IS_ODI_PROCESSED

                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_BUSINESS b
                            WHERE

                                    a.REFERENCE_DATA_NAME = 'BRANCH.COMPANY_NAME'
                                    AND a.MATCH_TYPE = 'WORD'

                                    AND (a.MATCH_VALUE like '% %' OR a.MATCH_VALUE like '%,%')
                                    AND CONTAINS (b.COMPANY_NAME,  '{' || a.MATCH_VALUE || '}',1) > 0
            UNION ALL
            SELECT   /*+ index(b,XIE2_COMPANY_NAME) */
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.COMPANY_NAME,

                                    a.MATCH_VALUE,
                                    b.IS_ODI_PROCESSED

                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_BUSINESS b
                            WHERE

                                    a.REFERENCE_DATA_NAME = 'BRANCH.COMPANY_NAME'
                                    AND a.MATCH_TYPE = 'WORD'

                                    AND (a.MATCH_VALUE not like '%,%' AND a.MATCH_VALUE not like '% %' AND a.MATCH_VALUE not like '*')
                                    AND CONTAINS (b.COMPANY_NAME,  '%' || a.MATCH_VALUE || '%',1) > 0;


            MERGE INTO STG_BUSINESS x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              NVL(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COMPANY_NAME, REPLACE_LIST, NULL,1,0,'i'),'^[^][)(a-zA-Z0-9\\s]+|[^]|[)(a-zA-Z0-9\\s\.]+$|\(\)|\*'),'\.{2,}$','.')),COMPANY_NAME) AS COMPANY_NAME,
                              CASE WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COMPANY_NAME, REPLACE_LIST, NULL,1,0,'i'),'^[^][)(a-zA-Z0-9\\s]+|[^]|[)(a-zA-Z0-9\\s\.]+$|\(\)|\*'),'\.{2,}$','.')) IS NULL THEN 'Y' ELSE 'N' END AS ERROR_FLAG
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.COMPANY_NAME,
                                    LISTAGG(b.REPLACE_LIST, '|') WITHIN GROUP (ORDER BY LENGTH(b.REPLACE_LIST) desc) AS REPLACE_LIST
                            FROM    GTT_CLEANUP_MATCH_DATA b
                            WHERE
                                    b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND b.IS_ODI_PROCESSED = 'N'
                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.COMPANY_NAME
                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.COMPANY_NAME = y.COMPANY_NAME , x.ERR_FLAG =y.ERROR_FLAG;


                EXECUTE IMMEDIATE ('TRUNCATE TABLE GTT_CLEANUP_MATCH_DATA');

            COMMIT;



          MERGE INTO STG_BUSINESS x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              NVL(TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COMPANY_NAME,REPLACE_LIST,NULL,1,0,'i'),REPLACE_LIST_1,NULL,1,0,'i'),'^[^][)(a-zA-Z0-9\\s]+|[^][)(a-zA-Z0-9\\s.]+$|\(\)|\*'),'\.{2,}$','.')),COMPANY_NAME) AS COMPANY_NAME,
							  CASE WHEN TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COMPANY_NAME,REPLACE_LIST,NULL,1,0,'i'),REPLACE_LIST_1,NULL,1,0,'i'),'^[^][)(a-zA-Z0-9\\s]+|[^][)(a-zA-Z0-9\\s.]+$|\(\)|\*'),'\.{2,}$','.')) IS NULL THEN 'Y' ELSE 'N' END  AS ERROR_FLAG
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.COMPANY_NAME,
                                    LISTAGG('^'||a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST,
                                    LISTAGG(a.MATCH_VALUE||'$', '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST_1
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_BUSINESS b
                            WHERE
                                    b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'BRANCH.COMPANY_NAME'
                                    AND a.MATCH_TYPE = 'STARTORENDSWITH'
                                    AND b.IS_ODI_PROCESSED = 'N'
									AND b.ERR_FLAG <> 'Y'
                                    AND regexp_like(UPPER(b.COMPANY_NAME),'^'||a.MATCH_VALUE||'|'||a.MATCH_VALUE||'$')
                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.COMPANY_NAME
                      ) t
                )  y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.COMPANY_NAME = y.COMPANY_NAME , x.ERR_FLAG =y.ERROR_FLAG;
        COMMIT;

		Update  STG_BUSINESS
		SET     COMPANY_NAME =  NVL(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COMPANY_NAME, '^[^][)(a-zA-Z0-9\\s]+|[^][)(a-zA-Z0-9\\s.]+$'),'\.{2,}$','.'),'*',NULL),COMPANY_NAME),
				ERR_FLAG = CASE WHEN TRIM(REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COMPANY_NAME, '^[^][)(a-zA-Z0-9\\s]+|[^][)(a-zA-Z0-9\\s.]+$'),'\.{2,}$','.'),'*',NULL)) IS NULL THEN 'Y' ELSE 'N' END
				WHERE   SOURCE_NAME = V_SOURCE_NAME
        			AND ENTITY_NAME= V_ENTITY_NAME
				AND NVL(ERR_FLAG, 'N') = 'N';

		COMMIT;
	ELSIF(V_ENTITY_NAME = 'NCM') THEN


         PKG_COMMON.SP_GATHER_STATS_ALL_TABLES (  PVI_TABLE_NAME => 'STG_PERSON') ;



        UPDATE  STG_PERSON SET IS_ACTIVE ='N' WHERE REGEXP_LIKE(upper(FIRST_NAME), '(^[Z]+?)\1')
        --OR REGEXP_LIKE(upper(MIDDLE_NAME), '(^[Z]+?)\1') 
        OR REGEXP_LIKE(upper(LAST_NAME), '(^[Z]+?)\1')  AND ENTITY_NAME='NCM';

					MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              REPLACE(REGEXP_REPLACE(FIRST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL) AS FIRST_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.FIRST_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'NCM.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND UPPER(b.FIRST_NAME) LIKE '%' || a.MATCH_VALUE || '%'
                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.FIRST_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.FIRST_NAME = y.FIRST_NAME;
				COMMIT;

				MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              REPLACE(REGEXP_REPLACE(MIDDLE_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL) AS MIDDLE_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.MIDDLE_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'NCM.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND UPPER(b.MIDDLE_NAME) LIKE '%' || a.MATCH_VALUE || '%'


                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.MIDDLE_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.MIDDLE_NAME = y.MIDDLE_NAME;
				COMMIT;

				MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              REPLACE(REGEXP_REPLACE(LAST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL) AS LAST_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.LAST_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'NCM.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND UPPER(b.LAST_NAME) LIKE '%' || a.MATCH_VALUE || '%'


                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.LAST_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.LAST_NAME = y.LAST_NAME;
				COMMIT;
        UPDATE STG_PERSON set
        	 FIRST_NAME = TRIM(REGEXP_REPLACE(FIRST_NAME,'[^a-zA-Z0-9 .,''-]+','')) ,
           MIDDLE_NAME= TRIM(REGEXP_REPLACE(MIDDLE_NAME,'[^a-zA-Z0-9 .,''-]+','')) ,
       		 LAST_NAME= TRIM(REGEXP_REPLACE(LAST_NAME,'[^a-zA-Z0-9 .,''-]+','')),
           ERR_FLAG = CASE WHEN (TRIM(REGEXP_REPLACE(FIRST_NAME,'[^a-zA-Z0-9 .,''-]+',''))||TRIM(REGEXP_REPLACE(MIDDLE_NAME,'[^a-zA-Z0-9 .,''-]+',''))||TRIM(REGEXP_REPLACE(LAST_NAME,'[^a-zA-Z0-9 .,''-]+','')))
           IS NULL THEN 'Y' ELSE 'N' END
        WHERE
       		SOURCE_NAME = V_SOURCE_NAME
       		AND ENTITY_NAME= V_ENTITY_NAME
          AND NVL(ERR_FLAG,'N') = 'N';

        COMMIT;

		ELSIF(V_ENTITY_NAME = 'ADJUSTER') THEN


           PKG_COMMON.SP_GATHER_STATS_ALL_TABLES (  PVI_TABLE_NAME => 'STG_PERSON') ;



        UPDATE  STG_PERSON SET IS_ACTIVE ='N' WHERE REGEXP_LIKE(upper(FIRST_NAME), '(^[Z]+?)\1')
        --OR REGEXP_LIKE(upper(MIDDLE_NAME), '(^[Z]+?)\1') 
        OR REGEXP_LIKE(upper(LAST_NAME), '(^[Z]+?)\1')  AND ENTITY_NAME='ADJUSTER';

			MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              REPLACE(REGEXP_REPLACE(FIRST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL) AS FIRST_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.FIRST_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM   C_EDQ_REFERENCE_DATA  a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
 	                                AND a.REFERENCE_DATA_NAME = 'ADJUSTER.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
									AND UPPER(b.FIRST_NAME) LIKE '%' || a.MATCH_VALUE || '%'
                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.FIRST_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME
                and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.FIRST_NAME = y.FIRST_NAME;
				COMMIT;
				MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              REPLACE(REGEXP_REPLACE(MIDDLE_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL) AS MIDDLE_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.MIDDLE_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'ADJUSTER.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND UPPER(b.MIDDLE_NAME) LIKE '%' || a.MATCH_VALUE || '%'


                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.MIDDLE_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.MIDDLE_NAME = y.MIDDLE_NAME;
				COMMIT;
				MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              REPLACE(REGEXP_REPLACE(LAST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL) AS LAST_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.LAST_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'ADJUSTER.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND UPPER(b.LAST_NAME) LIKE '%' || a.MATCH_VALUE || '%'


                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.LAST_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.LAST_NAME = y.LAST_NAME;
				COMMIT;
        UPDATE STG_PERSON set
          FIRST_NAME = TRIM(REGEXP_REPLACE(FIRST_NAME,'[^a-zA-Z0-9 .,''-]+','')) ,
          MIDDLE_NAME= TRIM(REGEXP_REPLACE(MIDDLE_NAME,'[^a-zA-Z0-9 .,''-]+','')) ,
       		LAST_NAME= TRIM(REGEXP_REPLACE(LAST_NAME,'[^a-zA-Z0-9 .,''-]+','')),
          ERR_FLAG = CASE WHEN (TRIM(REGEXP_REPLACE(FIRST_NAME,'[^a-zA-Z0-9 .,''-]+',''))||TRIM(REGEXP_REPLACE(MIDDLE_NAME,'[^a-zA-Z0-9 .,''-]+',''))||TRIM(REGEXP_REPLACE(LAST_NAME,'[^a-zA-Z0-9 .,''-]+',''))) IS NULL THEN 'Y' ELSE 'N' END,
          ERR_REASON = NULL
        WHERE
           SOURCE_NAME = V_SOURCE_NAME
           AND ENTITY_NAME= V_ENTITY_NAME
           AND NVL(ERR_FLAG,'N') = 'N';



        COMMIT;

-- =============================================================================
-- For PATIENT ENTITY


        ELSIF(V_ENTITY_NAME = 'PATIENT') THEN

PKG_COMMON.SP_GATHER_STATS_ALL_TABLES (  PVI_TABLE_NAME => 'STG_PERSON') ;
				MERGE INTO STG_PERSON x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(FIRST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) AS FIRST_NAME,
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(MIDDLE_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) AS MIDDLE_NAME,
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(LAST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) AS LAST_NAME,
                              CASE WHEN TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(FIRST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) ||
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(MIDDLE_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) ||
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(LAST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) IS NULL THEN 'Y' ELSE 'C' END ERR_FLAG,
                              CASE WHEN TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(FIRST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) ||
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(MIDDLE_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) ||
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(LAST_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) IS NULL THEN 'Failed at stripping logic'  END ERR_REASON

                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.FIRST_NAME,
                                    b.MIDDLE_NAME,
                                    b.LAST_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_PERSON b
                            WHERE   b.IS_ODI_PROCESSED = 'N'
                                    AND NVL(b.ERR_FLAG,'N') = 'C'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'PATIENT.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND (UPPER(b.FIRST_NAME) LIKE '%' || a.MATCH_VALUE || '%' OR UPPER(b.MIDDLE_NAME) LIKE '%' || a.MATCH_VALUE || '%' OR UPPER(b.LAST_NAME) LIKE '%' || a.MATCH_VALUE || '%')
                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.FIRST_NAME,
                                    b.MIDDLE_NAME,
                                    b.LAST_NAME

                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.FIRST_NAME = y.FIRST_NAME,x.MIDDLE_NAME=y.MIDDLE_NAME,x.LAST_NAME=y.LAST_NAME,x.ERR_FLAG=y.ERR_FLAG,x.ERR_REASON=y.ERR_REASON;

				COMMIT;

-- =============================================================================
--CLAIM
--==========
ELSIF(V_ENTITY_NAME = 'CLAIM') THEN

			PKG_COMMON.SP_GATHER_STATS_ALL_TABLES (  PVI_TABLE_NAME => 'STG_CLAIM') ;
            
            	MERGE INTO STG_CLAIM x
                USING
                (
                      SELECT  SOURCE_NAME,
                              ENTITY_NAME,
                              SOURCE_REF,
                              TRIM(REGEXP_REPLACE(REPLACE(REGEXP_REPLACE(ATTORNEY_NAME, REPLACE_LIST, NULL,1,0,'i'),'*',NULL),'[^a-zA-Z0-9 .,''-]+','')) AS ATTORNEY_NAME
                      FROM
                      (
                            SELECT  b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.ATTORNEY_NAME,
                                    LISTAGG(a.MATCH_VALUE, '|') WITHIN GROUP (ORDER BY LENGTH(a.MATCH_VALUE) desc) as REPLACE_LIST
                            FROM    C_EDQ_REFERENCE_DATA a
                                        CROSS JOIN STG_CLAIM b
                            WHERE   b.IS_ODI_PROCESSED = 'Y'
                                    AND b.SOURCE_NAME = V_SOURCE_NAME
                                    AND b.ENTITY_NAME= V_ENTITY_NAME
                                    AND a.REFERENCE_DATA_NAME = 'CLAIM.NAME'
                                    AND a.MATCH_TYPE = 'WORD'
                                    AND (UPPER(b.ATTORNEY_NAME) LIKE '%' || a.MATCH_VALUE || '%')
                            GROUP BY
                                    b.SOURCE_NAME,
                                    b.ENTITY_NAME,
                                    b.SOURCE_REF,
                                    b.ATTORNEY_NAME


                      ) t
                ) y ON (y.SOURCE_NAME = x.SOURCE_NAME and y.ENTITY_NAME = x.ENTITY_NAME and y.SOURCE_REF = x.SOURCE_REF)
                WHEN MATCHED THEN UPDATE SET x.ATTORNEY_NAME = y.ATTORNEY_NAME;

				COMMIT;

--=============


  END IF;
 IF ( V_ENTITY_NAME = 'BRANCH' OR V_ENTITY_NAME='NCM' OR V_ENTITY_NAME = 'ADJUSTER' OR V_ENTITY_NAME = 'PATIENT') THEN
        PKG_ODI.SP_ODI_DATA_VALIDATION (  PVI_ENTITY_NAME => V_ENTITY_NAME,PVI_SOURCE_NAME => V_SOURCE_NAME) ;
  END IF;
  END SP_RM_STR_BRANCH_COMP_NAME;

  PROCEDURE SP_ODI_DATA_VALIDATION
  (
      PVI_ENTITY_NAME IN C_ODI_ENTITY_METADATA.ENTITY_NAME%TYPE,
      PVI_SOURCE_NAME IN VARCHAR2
  )
    IS
        N_ODI_ENTITY_METADATA_RID 	C_ODI_ENTITY_METADATA.ODI_ENTITY_METADATA_RID%TYPE;
        V_STG_TABLE_NAME 			      C_ODI_ENTITY_METADATA.STG_TABLE_NAME%TYPE;
        V_STG_VIEW_NAME 			      C_ODI_ENTITY_METADATA.STG_VIEW_NAME%TYPE;
        V_VALID_INVALID_COL_NAME	  C_ODI_ENTITY_METADATA.VALID_INVALID_COL_NAME%TYPE;
        V_INVALID_REASON_COL_NAME	  C_ODI_ENTITY_METADATA.INVALID_REASON_COL_NAME%TYPE;
        V_STG_RID_COL_NAME			    C_ODI_ENTITY_METADATA.STG_RID_COL_NAME%TYPE;
        V_SQL 						          VARCHAR2(32767);
        N_ODI_ENTITY_VALIDATION_RID C_ODI_ENTITY_VALIDATION.ODI_ENTITY_VALIDATION_RID%TYPE;
        N_ROW_ID 					          NUMBER(12,0);
        V_ODI_COL_VALIDATION_RULE	  C_ODI_ENTITY_VALIDATION.ODI_COL_VALIDATION_RULE%TYPE;
        C1                          SYS_REFCURSOR;
        V_VAR_COL_LIST              VARCHAR2(4000);
        V_COL_LIST                  VARCHAR2(4000);
        V_ERR_DESC                  VARCHAR2(4000);
        V_SQL1                      VARCHAR2(4000);
    BEGIN
        /*
            Init
        */
        SELECT  ODI_ENTITY_METADATA_RID,
                STG_TABLE_NAME,
                STG_VIEW_NAME,
                VALID_INVALID_COL_NAME,
                INVALID_REASON_COL_NAME,
                STG_RID_COL_NAME
        INTO    N_ODI_ENTITY_METADATA_RID,
                V_STG_TABLE_NAME,
                V_STG_VIEW_NAME,
                V_VALID_INVALID_COL_NAME,
                V_INVALID_REASON_COL_NAME,
                V_STG_RID_COL_NAME
        FROM    C_ODI_ENTITY_METADATA
        WHERE   ENTITY_NAME = PVI_ENTITY_NAME;

        /*
            Denoise Zip_plus_four in STG_ADDRESS
        */
        UPDATE STG_ADDRESS SET ZIP_PLUS_FOUR=REGEXP_REPLACE(TRIM(REPLACE(ZIP_PLUS_FOUR,'-',' ')), '\s{1,}', '-')
        WHERE   ENTITY_NAME = PVI_ENTITY_NAME
              AND SOURCE_NAME = PVI_SOURCE_NAME;

        V_SQL := 'SELECT '|| V_STG_RID_COL_NAME;
        DBMS_OUTPUT.PUT_LINE(V_SQL);
        FOR i IN
        (
            SELECT  ODI_COL_NAME AS COL_NAME,
                    ODI_COL_VALIDATION_RULE
            FROM    C_ODI_ENTITY_VALIDATION
            WHERE   ODI_ENTITY_METADATA_ID = N_ODI_ENTITY_METADATA_RID
            AND 	IS_ACTIVE = 'Y'
        ) LOOP
                  V_SQL := V_SQL ||','|| ' CASE WHEN '||i.COL_NAME||' IS NULL THEN '''' WHEN '||i.ODI_COL_VALIDATION_RULE ||' THEN '||''''||i.COL_NAME||','''||' ELSE '''' END AS '|| i.COL_NAME;
                --  DBMS_OUTPUT.PUT_LINE(V_SQL);
                  V_COL_LIST := CASE WHEN V_COL_LIST IS NULL THEN '' ELSE V_COL_LIST||'||''''||'END||i.COL_NAME;

        END LOOP;

                  V_SQL := V_SQL || ' FROM '|| V_STG_VIEW_NAME || ' WHERE SOURCE_NAME = '||''''||PVI_SOURCE_NAME||''''||' AND ENTITY_NAME = '||''''||PVI_ENTITY_NAME||'''';
                  V_SQL := 'SELECT '||V_STG_RID_COL_NAME||','||V_COL_LIST||' AS DESC_LIST FROM ( '||V_SQL||')';
                  --DBMS_OUTPUT.PUT_LINE(V_SQL);

                  IF (PVI_ENTITY_NAME = 'PATIENT') THEN

                V_SQL1 := ' MERGE INTO '||V_STG_TABLE_NAME||' S
                            USING ('||V_SQL||') T
                            ON (S.'||V_STG_RID_COL_NAME||'=T.'||V_STG_RID_COL_NAME||')
                            WHEN MATCHED THEN
                            UPDATE SET S.'||V_VALID_INVALID_COL_NAME||'=CASE WHEN LENGTH(T.DESC_LIST) > 0 THEN ''Y'' ELSE ''C'' END,
                            S.'||V_INVALID_REASON_COL_NAME||'=CASE WHEN LENGTH(T.DESC_LIST) > 0 THEN SUBSTR(T.DESC_LIST, 1, LENGTH(T.DESC_LIST)-1) ELSE NULL END
                            WHERE ENTITY_NAME    = '''||PVI_ENTITY_NAME||'''
                            and
                            SOURCE_NAME      = '''||PVI_SOURCE_NAME||'''
                            AND S.'||V_VALID_INVALID_COL_NAME||' = ''C'''
                            ;
				  ELSE

                  V_SQL1 := ' MERGE INTO '||V_STG_TABLE_NAME||' S
                            USING ('||V_SQL||') T
                            ON (S.'||V_STG_RID_COL_NAME||'=T.'||V_STG_RID_COL_NAME||')
                            WHEN MATCHED THEN
                            UPDATE SET S.'||V_VALID_INVALID_COL_NAME||'=CASE WHEN LENGTH(T.DESC_LIST) > 0 THEN ''Y'' ELSE S.'||V_VALID_INVALID_COL_NAME||' END, S.'||V_INVALID_REASON_COL_NAME||'=CASE WHEN LENGTH(T.DESC_LIST) > 0 THEN SUBSTR(T.DESC_LIST, 1, LENGTH(T.DESC_LIST)-1) ELSE NULL END
                            WHERE ENTITY_NAME    = '''||PVI_ENTITY_NAME||'''
                            AND SOURCE_NAME      = '''||PVI_SOURCE_NAME||'''';
                  END IF;

                 --DBMS_OUTPUT.PUT_LINE(V_SQL1);--- changes by vijay @ 1586-->1588 and commented line no-->1589
                -- IF (PVI_ENTITY_NAME = 'PATIENT') THEN
                 EXECUTE IMMEDIATE V_SQL1;
               --  END IF;
                -- EXECUTE IMMEDIATE V_SQL1;

                 COMMIT;
             --    DBMS_OUTPUT.PUT_LINE(V_SQL1);
     END;
END;
/