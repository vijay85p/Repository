CDC IN ODI
==========

Step:-1
-------
Create table (temp)
create table STAGING.I$_CDC_TGT
(
	EMPNO		NUMBER(4) NULL,
	ENAME		VARCHAR2(10) NULL,
	JOB		VARCHAR2(9) NULL,
	SAL		NUMBER(7,2) NULL,
	DEPTNO		NUMBER(2) NULL,
	JRN_SUBSCRIBER		VARCHAR2(50) NULL,
	JRN_FLAG		VARCHAR2(1) NULL,
	JRN_DATE		DATE NULL,
	IND_UPDATE		CHAR(1)
)
NOLOGGING

Step:-2 
Lock Journalized TABLE
update	STAGING.J$CDC_SRC set	JRN_CONSUMED = '1' where	(1=1)  And JRN_SUBSCRIBER = 'SUNOPSIS';