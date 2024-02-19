select job ,avg(sal) from emp group by job having  avg(sal)>1500;

select	EMP.EMPNO	   EMPNO,	EMP.ENAME	   ENAME,	EMP.JOB	   JOB
from	SCOTT.EMP   EMP where	(1=1)
Minus
select	EMP.EMPNO	   EMPNO,	EMP.ENAME	   ENAME,	EMP.JOB	   JOB
from	SCOTT.EMP   EMP where	(1=1)

create sequence seq1 start with 1 increment by 1;
select seq1.nextval from dual;
select seq1.currval from dual;

select  to_number(to_char(sysdate,'yyy')) from dual;
select * from emp;
select * from emp order by job;


SELECT deptno, sal
FROM (
    SELECT deptno, sal, ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY sal DESC) AS salary_rank
    FROM emp WHERE sal IS NOT NULL
) ranked_salaries
WHERE salary_rank = 3;

select emp.*, rank() over(order by sal desc )rk,
dense_rank() over(order by sal desc )drk,row_number() over(order by sal desc )rn from emp;-- group by deptno;

select deptno,sum(deptno) from emp group by deptno;
select deptno,count(2) from emp group by deptno having count(empno)>1 order by deptno;
select max(ename),min(sal), max(deptno),sal from emp group by sal order by sal;

CREATE TABLE CDC_SRC AS select * from SCOTT.EMP;
ALTER TABLE CDC_SRC ADD CONSTRAINT PK_CDC_SRC_EMPMO PRIMARY KEY (EMPNO);


CREATE TABLE CDC_TGT AS select * from SCOTT.EMP WHERE 1=0;
ALTER TABLE CDC_TGT ADD CONSTRAINT PK_CDC_TGT_EMPMO PRIMARY KEY (EMPNO);

DROP TABLE STAGING.table_name [CASCADE CONSTRAINTS | PURGE];
drop FROM all_tables  WHERE table_name LIKE 'C$___%'
drop table FROM all_tables  WHERE table_name LIKE 'TEST_%'
DROP TABLE  from all_tablesSTAGING.I$_SCD1_TGT PURGE;

select * from emp;
SELECT AVG(sal) FROM   emp;
SELECT deptno, AVG(sal) FROM   emp GROUP BY deptno ORDER BY deptno;

SELECT empno, deptno, sal, AVG(sal) OVER (PARTITION BY deptno) AS avg_dept_sal FROM   emp;
SELECT empno, deptno, sal, AVG(sal) OVER (PARTITION BY sal) AS avg_dept_sal FROM   emp;
SELECT empno, deptno, sal, AVG(sal) OVER (ORDER BY sal) AS avg_sal FROM   emp where deptno=10;

SELECT ename,empno, deptno, sal,FIRST_VALUE(sal IGNORE NULLS) OVER (PARTITION BY deptno order by deptno) AS first_sal_in_dept FROM   emp;
SELECT empno, deptno, sal,FIRST_VALUE(sal IGNORE NULLS) OVER (PARTITION BY deptno ORDER BY sal ASC NULLS FIRST) AS first_val_in_dept FROM emp;

SELECT empno, deptno, sal, AVG(sal) OVER (PARTITION BY deptno ORDER BY sal ROWS BETWEEN 0 PRECEDING AND 0 PRECEDING) AS avg_of_current_sal FROM   emp;

SELECT empno, deptno, sal, AVG(sal) OVER (PARTITION BY deptno ORDER BY sal) AS avg_dept_sal_sofar FROM   emp;
SELECT empno, deptno, sal, AVG(sal) OVER (PARTITION BY deptno ORDER BY SAL) AS avg_dept_sal FROM   emp;

SELECT empno, deptno, sal, 
FIRST_VALUE(sal) OVER (ORDER BY sal ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS previous_sal,
LAST_VALUE(sal) OVER (ORDER BY sal ROWS BETWEEN CURRENT ROW AND 1 FOLLOWING) AS next_sal FROM   emp;
       
       
SELECT ename, sal,deptno, COUNT(*) OVER (ORDER BY sal RANGE BETWEEN 50 PRECEDING AND 150 FOLLOWING) AS mov_count
FROM emp  ORDER BY deptno;       


DELETE
======

DELETE FROM your_table
WHERE ROWID NOT IN (
    SELECT MIN(ROWID)
    FROM your_table
    GROUP BY col1, col2
);


=====================

To find the duplicates records in a table

create table pets(petid number(3),petname varchar2(20),pettype varchar2(20));
  select * from pets;
  insert into pets values(4,'Bark','Dog');
  
  SELECT PetId,PetName,PetType, COUNT(*) AS Count FROM Pets GROUP BY PetId,PetName,PetType 
  having Count(*)>1 ORDER BY Count desc;
  
  SELECT rownum,rowid,PetId,PetName,PetType, 
  ROW_NUMBER() OVER (PARTITION BY PetId, PetName, PetType ORDER BY PetId, PetName, PetType)
  AS rn FROM Pets;
  
  SELECT rownum,rowid,PetId,PetName,PetType, 
  ROW_NUMBER() OVER (PARTITION BY PetName ORDER BY PetName) AS rn FROM Pets;
  
  WITH cte AS 
    (
        SELECT PetId,PetName,PetType,
            ROW_NUMBER() OVER (PARTITION BY PetId,PetName,PetType ORDER BY PetId,PetName,PetType
							  ) AS Row_Number FROM Pets
    )
SELECT * FROM cte WHERE Row_Number <> 1;

SELECT rowid,Pets.* FROM Pets WHERE   EXISTS (SELECT 1 FROM Pets p2 WHERE Pets.PetName = p2.PetName  AND Pets.PetType = p2.PetType AND 
Pets.rowid > p2.rowid );

SELECT * FROM Pets WHERE rowid > (SELECT MIN(rowid) FROM Pets p2 WHERE Pets.PetName = p2.PetName AND Pets.PetType = p2.PetType
);


select e.*, count(*) over (partition by empno) number_of_occurence from emp25 e;

with item_count as (select orders.*, count(*) over (partition by empno) number_of_occurence
                    from emp25 orders)
select *
from item_count
where number_of_occurence > 1;




===============
Merge Statement
 MERGE INTO EMP1 T
USING EMP S
ON (S.EMPNO = T.EMPNO)
WHEN MATCHED THEN
  UPDATE SET T.ENAME = S.ENAME, T.SAL = S.SAL
WHEN NOT MATCHED THEN
  INSERT (EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO)
  VALUES (S.EMPNO, S.ENAME, S.JOB, S.MGR, S.HIREDATE, S.SAL, S.COMM, S.DEPTNO);
						   

====================
In Refreshing tab:- 
SELECT JOB_RID FROM <?=odiRef.getObjectName("L", "C_ODI_JOB_METADATA", "OCCM_MDM_LS", "", "D") ?> WHERE JOB_NAME = #ODI_JOB_NAME						   

update value with case
======================

update gender set gen=case when gen='F' then 'M'
                           when gen='M' then 'F' else 'TG' end;
						   
=======================


Pivot & UNPIVOT
===============

select * from pivot1 ORDER BY location, location;
SELECT location, customer_id, SUM(sales_amount)
FROM pivot1
GROUP BY location, customer_id
ORDER BY location, location;

SELECT *
FROM pivot1
PIVOT (
  SUM(sales_amount)
  FOR customer_id
  IN (1, 2, 3, 4, 5, 6) );
  
  
  SELECT *
FROM (
  SELECT location, customer_id , sales_amount
  FROM pivot1
)
PIVOT (
  SUM(sales_amount)
  FOR customer_id IN (1, 2, 3, 4)
);

----------------Aliasing 
SELECT *
FROM pivot1
PIVOT (
  SUM(sales_amount) as sales_total
  FOR customer_id
  IN (1 as id1, 2, 3, 4, 5, 6) );
  
  SELECT *
FROM (
  SELECT location, customer_id, sale_amount
  FROM cust_sales_category
)
PIVOT (
  SUM(sale_amount) AS sum_sales,
  COUNT(sale_amount) AS count_sales
  FOR customer_id
  IN (1, 2, 3, 4)
);

Left outer join (but need inner join result set)
================================================
select e.*,d.* from emp e right join  dept d on e.deptno=d.deptno  and
(select deptno from emp e,deptno d  e.deptno=d.deptno ) where order by d.deptno desc ;

select e.*,d.* from emp e right join  dept d on e.deptno=d.deptno  --and
 where-- e.deptno is not null or 
 e.deptno in (select deptno from emp) order by d.deptno desc ;
 
 select a.*,b.* from dept a left outer join  emp b on a.deptno=b.deptno  where   a.deptno IS NOT NULL and b.deptno is not null;