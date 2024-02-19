SQL Command
===========
select * from emp order by deptno,sal;
select max(sal) from emp;--Get max Sal


Toget highest Sal Dept wise
---------------------------
select max(sal),deptno from emp group by deptno having(max(sal)>=3000);
SELECT deptno, MAX(sal) AS second_highest_salary
FROM (
  SELECT deptno, sal, RANK() OVER (PARTITION BY deptno ORDER BY sal desc) AS salary_rank
  FROM emp WHERE sal IS NOT NULL-- WHERE RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) = 2
     ) R
WHERE salary_rank = 2 GROUP BY deptno;

SELECT deptno, MAX(sal) AS second_highest_salary
FROM (
  SELECT deptno, sal, Dense_RANK() OVER (PARTITION BY deptno ORDER BY sal desc) AS salary_rank
  FROM emp WHERE sal IS NOT NULL-- WHERE RANK() OVER (PARTITION BY deptno ORDER BY sal DESC) = 2
     ) R
WHERE salary_rank = 2 GROUP BY deptno;

---Dynamically---
select * from emp where deptno=20 order by sal ;

SELECT sal,deptno
FROM (  SELECT deptno,sal, ROW_NUMBER() OVER (ORDER BY sal DESC) AS row_num  FROM emp  WHERE deptno = 20 )ranked_employees
WHERE row_num = '&n';


SELECT sal,deptno
FROM (SELECT deptno,sal, ROW_NUMBER() OVER (ORDER BY sal DESC) AS row_num  FROM emp where sal is not null -- WHERE deptno = 20
)ranked_employees
WHERE row_num = '&n';


Duplicate records
==================
SELECT deptno, COUNT(deptno) AS duplicate_count FROM emp GROUP BY deptno HAVING COUNT(deptno) > 1;

SELECT *
FROM ( SELECT deptno,empno, ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY deptno) AS row_num FROM emp) 
WHERE row_num > 1;

Duplicates on multiple columsn
===============================
SELECT  officer_name,posting_location, COUNT(*) AS ALIAS
FROM postings GROUP BY  officer_name, posting_location HAVING COUNT(*)>1;

WITH cte AS 
    (
        SELECT  officer_name, team_size,
        posting_location, ROW_NUMBER() OVER (PARTITION BY officer_name, team_size,posting_location ORDER BY officer_name, team_size,
        posting_location) AS Row_Number
        FROM postings
    )
SELECT * FROM cte WHERE Row_Number <> 1;

SELECT P1.*,ROWID FROM POSTINGS P1
WHERE EXISTS (SELECT 1 FROM POSTINGS p2 WHERE P1.officer_name = p2.officer_name
  AND P1.posting_location = p2.posting_location AND P1.TEAM_SIZE=P2.TEAM_SIZE
  AND P1.rowid > p2.rowid
);

SELECT P1.*,ROWID FROM EMP P1
WHERE ROWID > (SELECT MIN(ROWID) FROM EMP p2 
  WHERE --P1.EMPNO = p2.EMPNO AND
  P1.DEPTNO = p2.DEPTNO AND P1.SAL=P2.SAL  --AND 
 --P1.rowid < p2.rowid GROUP BY P1.DEPTNO
);

Original Records
================
SELECT *
FROM ( SELECT deptno,empno, ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY deptno) AS row_num FROM emp) 
WHERE row_num = 1;

WITH cte AS 
    (
        SELECT  officer_name, team_size,
        posting_location, ROW_NUMBER() OVER (PARTITION BY officer_name, team_size,posting_location ORDER BY officer_name, team_size,
        posting_location) AS Row_Number
        FROM postings
    )
SELECT * FROM cte WHERE Row_Number = 1;

SELECT P1.*,ROWID FROM POSTINGS P1
WHERE EXISTS (SELECT 1 FROM POSTINGS p2 WHERE P1.officer_name = p2.officer_name AND P1.posting_location = p2.posting_location 
					AND P1.TEAM_SIZE=P2.TEAM_SIZE AND P1.rowid < p2.rowid
);


UPDATE SINGLE COLUMNS
=====================

SELECT E.*,d.* FROM EMP E
LEFT OUTER JOIN DEPT D
ON E.DEPTNO = COALESCE(D.DEPTNO, -1) AND E.DEPTNO = 10;


========================
create table <%=odiRef.getTable("L", "TARG_NAME1", "A")%>
(
	<%=odiRef.getTargetColList("", "[COL_NAME]\t[DEST_CRE_DT] " + odiRef.getInfo("DEST_DDL_NULL"), ",\n\t", "")%>
)

======================
VARRAY

CREATE TYPE phone_numbers AS VARRAY(3) OF VARCHAR2(15);

CREATE TABLE person (
    person_id NUMBER PRIMARY KEY,
    person_name VARCHAR2(50),
    contact_numbers phone_numbers
);

INSERT INTO person VALUES (1, 'John Doe', phone_numbers('123-456-7890', '987-654-3210'));

SELECT person_id, person_name, COLUMN_VALUE AS contact_number
FROM person,
     TABLE(contact_numbers);

