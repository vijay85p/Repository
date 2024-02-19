BEGIN
DELETE FROM <%=odiRef.getObjectName( "L" , "TEMP" , "D" )%>  WHERE DEPTNO=10;
--DELETE FROM SCOTT.TEMP WHERE DEPTNO=10;
COMMIT;


CREATE TABLE <%=odiRef.getTable("L", "INT_NAME", "A")%>
(<%=odiRef.getColList("", "\t[COL_NAME] [DEST_CRE_DT]", ",\n", "", "")%>

startscen.bat M_FIRST 001 DEVSCOTT "-v=5"
https://deloitte.zoom.us/j/91887122212?pwd=

UFZZenEzNThYSEd1WU9XMTU0QjdPQT09

MERGE INTO target_table
USING source_table
ON (condition)
WHEN MATCHED THEN
  UPDATE SET column1 = value1, column2 = value2, ...
WHEN NOT MATCHED THEN
  INSERT (column1, column2, ...)
  VALUES (value1, value2, ...)
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;

CREATE SEQUENCE seq_name
  START WITH 1
  INCREMENT BY 1
  MINVALUE 1
  MAXVALUE 1000
  NOCACHE;