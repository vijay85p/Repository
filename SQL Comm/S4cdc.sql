  CREATE OR REPLACE FORCE EDITIONABLE VIEW "STAGING"."JV$EMP" 
  ("JRN_FLAG", "JRN_SUBSCRIBER", "JRN_DATE", 
   "EMPNO", "ENAME", "JOB", "MGR", "HIREDATE", "SAL", "COMM", "DEPTNO") 
   AS 
	select 	decode(TARG.ROWID, null, 'D', 'I')	   JRN_FLAG,
	JRN.JRN_SUBSCRIBER		   					   JRN_SUBSCRIBER,
	JRN.JRN_DATE		     					   JRN_DATE,
	JRN.EMPNO		   							   EMPNO,
	TARG.ENAME		   ENAME,
	TARG.JOB		   JOB,
	TARG.MGR		   MGR,
	TARG.HIREDATE	   HIREDATE,
	TARG.SAL		   SAL,
	TARG.COMM		   COMM,
	TARG.DEPTNO		   DEPTNO
from	(
		select	L.JRN_SUBSCRIBER	   JRN_SUBSCRIBER,
			L.EMPNO	   EMPNO,
			max(L.JRN_DATE)	   JRN_DATE
		from	STAGING.J$EMP    L
		where	L.JRN_CONSUMED = '1'
		group by	L.JRN_SUBSCRIBER,L.EMPNO
	)    JRN, SCOTT.EMP    TARG
				where	JRN.EMPNO	= TARG.EMPNO (+) ;


 
  CREATE OR REPLACE FORCE EDITIONABLE VIEW "STAGING"."JV$CDC_SRC" ("JRN_FLAG", "JRN_SUBSCRIBER", "JRN_DATE", "EMPNO", "ENAME", "JOB", "MGR", "HIREDATE", "SAL", "COMM", "DEPTNO") AS 
  select 	decode(TARG.ROWID, null, 'D', 'I')	   JRN_FLAG,
	JRN.JRN_SUBSCRIBER		   JRN_SUBSCRIBER,
	JRN.JRN_DATE		   JRN_DATE,
	JRN.EMPNO		   EMPNO,
	TARG.ENAME		   ENAME,
	TARG.JOB		   JOB,
	TARG.MGR		   MGR,
	TARG.HIREDATE		   HIREDATE,
	TARG.SAL		   SAL,
	TARG.COMM		   COMM,
	TARG.DEPTNO		   DEPTNO
from	(
		select	L.JRN_SUBSCRIBER	   JRN_SUBSCRIBER,
			L.EMPNO	   EMPNO,
			max(L.JRN_DATE)	   JRN_DATE
		from	STAGING.J$CDC_SRC    L
		where	L.JRN_CONSUMED = '1'
		group by	L.JRN_SUBSCRIBER,
			L.EMPNO
	)    JRN,
	SCOTT.CDC_SRC    TARG
where	JRN.EMPNO	= TARG.EMPNO (+) ;



  CREATE OR REPLACE FORCE EDITIONABLE VIEW "STAGING"."JV$DCDC_SRC" ("JRN_FLAG", "JRN_SUBSCRIBER", "JRN_DATE", "EMPNO", "ENAME", "JOB", "MGR", "HIREDATE", "SAL", "COMM", "DEPTNO") AS 
  select 	decode(TARG.ROWID, null, 'D', 'I')	   JRN_FLAG,
	JRN.JRN_SUBSCRIBER		   JRN_SUBSCRIBER,
	JRN.JRN_DATE		   JRN_DATE,
	JRN.EMPNO		   EMPNO,
	TARG.ENAME		   ENAME,
	TARG.JOB		   JOB,
	TARG.MGR		   MGR,
	TARG.HIREDATE		   HIREDATE,
	TARG.SAL		   SAL,
	TARG.COMM		   COMM,
	TARG.DEPTNO		   DEPTNO
from	(
		select	L.JRN_SUBSCRIBER	   JRN_SUBSCRIBER,
			L.EMPNO	   EMPNO,
			max(L.JRN_DATE)	   JRN_DATE
		from	STAGING.J$CDC_SRC    L
		group by 	L.JRN_SUBSCRIBER,
			L.EMPNO
	)    JRN,
	SCOTT.CDC_SRC    TARG
where	JRN.EMPNO	= TARG.EMPNO (+);
