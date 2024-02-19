BEGIN
    DBMS_UTILITY.COMPILE_SCHEMA ('&1.');
END;
/

SET ECHO OFF
SET FEEDBACK OFF

PROMPT
PROMPT ======================================================================================================
PROMPT COMPILATION ERRORS
PROMPT ======================================================================================================

COLUMN OBJECT_NAME HEADING "OBJECT NAME" FORMAT A30 TRUNCATE
COLUMN LINE_POSITION HEADING "LINE/POS" FORMAT A10 TRUNCATE
COLUMN ERROR_TEXT HEADING "ERROR TEXT" FORMAT A60 WORD_WRAPPED

SELECT V.OBJECT_NAME,
       V.LINE_POSITION,
       V.ERROR_TEXT
  FROM (SELECT UE.NAME AS OBJECT_NAME,
               UE.TYPE AS OBJECT_TYPE,
               UE.LINE || '/' || UE.POSITION AS LINE_POSITION,
               UE.TEXT AS ERROR_TEXT,
               UE.SEQUENCE
          FROM USER_ERRORS UE
         WHERE UE.ATTRIBUTE = 'ERROR'
        UNION ALL
        SELECT 'NO ERRORS' AS OBJECT_NAME,
               NULL AS OBJECT_TYPE,
               NULL AS LINE_POSITION,
               NULL AS ERROR_TEXT,
               NULL AS SEQUENCE
          FROM DUAL
         WHERE NOT EXISTS (SELECT NULL
                             FROM USER_ERRORS UE
                            WHERE UE.ATTRIBUTE = 'ERROR')
        ORDER BY OBJECT_NAME,
                 OBJECT_TYPE,
                 SEQUENCE DESC) V;

PROMPT
PROMPT ======================================================================================================
