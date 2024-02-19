select * from emp1 order by deptno
select max(sal),min(sal),deptno from emp1 where deptno=30 group by deptno
select e.* from emp1 e where rowid=(select min(rowid) from emp1)

select e.* from emp1 e where rowid in(select max(rowid) from emp1 group by deptno)
select e.* from emp1 e where rowid in(select min(rowid) from emp1 group by deptno)

select * from emp1

select e.* from emp1 e where rowid in(select min(rowid) from emp1 f where e.deptno=f.deptno)
select * from emp1 e where rowid not in(select min(rowid) from emp1 group by deptno )


select e.* from emp1 e where rowid in(select min(rowid) from emp1 group by deptno)
select distinct (a.sal) from emp1 a where &n=(select count(distinct(b.sal)) from emp1 b where a.sal>=b.sal)
select * from (select * from emp1 order by sal desc) where rownum<=3 order by sal 


select * from emp1 where deptno=10 and 
select e.* ,rownum() over (order by sal desc) from emp1 e where deptno=20 and (rownum()=1 and rownum()<=3)

select * from emp1
create table dup as select * from emp1 where rowid not in(select max(rowid) from emp1 group by deptno)
rollback
select ename deptno from emp1 where rownum<6

select * from (select rownum r ,e.* from emp1 e) where r>5 and r<11 GROUP BY DEPTNO
select * from (select rownum r ,e.* from emp1 e) where r=5
SELECT * FROM (SELECT * FROM EMP1 ORDER BY SAL DESC) WHERE ROWNUM<=3

SELECT ROWNUM AS RANK , SAL,DEPTNO,ENAME FROM (SELECT DEPTNO FROM EMP1 ORDER BY SAL  GROUP BY DEPTNO) WHERE ROWNUM<=2
G:\VijayK\ODI Stufs\ODI\ODI & SQL  MATERIAL
======================================================================
select * from emp e;
select sales1.deptno, sales1.sal,sales1.empno  from 
(
select sales.deptno,sum(sales.sal) sal, sales.empno from scott.emp sales where (1=1)group by sales.deptno
) sales1;

select distinct deptno ,ename,sal,rownum from emp e where 5 <= (select count(ename) from emp where
             e.deptno = deptno);  5<=15
             
 select deptno,count(*)from emp group by deptno having count(*) > 4;            
 select deptno,ename, count(*) from emp group by deptno,ename having count(*) > 4;
 
 select deptno,ename from emp e1 where exists (
 select * from emp e2 where e1.deptno=e2.deptno group by e2.deptno having count(e2.ename) > 4) order by
 deptno,ename;
             
select deptno,ename from emp e1 where not exists (
select * from emp e2 where e1.deptno=e2.deptno group by e2.deptno having count(e2.ename) > 4) order by
deptno,ename;

==============================

Analytical Function or Window Function
======================================
DENSE_RANK

==========


Show the number of employees who joined in the past three years.
SELECT COUNT(*) AS num_employees_joined_last_three_years
FROM employees
WHERE EXTRACT(YEAR FROM join_date) >= EXTRACT(YEAR FROM CURRENT_DATE) - 3;

Self Join 
=====
CREATE TABLE Student (
    student_id INT PRIMARY KEY,
    student_name VARCHAR(50),
    score INT
);

INSERT INTO Student VALUES (1, 'Alice', 85);
INSERT INTO Student VALUES (2, 'Bob', 92);
INSERT INTO Student VALUES (3, 'Charlie', 88);
INSERT INTO Student VALUES (4, 'David', 92);
INSERT INTO Student VALUES (5, 'Eva', 79);
INSERT INTO Student VALUES (6, 'Frank', 88);

SELECT DISTINCT s1.score AS fourth_highest_score FROM Student s1
WHERE 4 = (    SELECT COUNT(DISTINCT s2.score)     FROM Student s2     WHERE s2.score >= s1.score);

| fourth_highest_score |
|-----------------------|
| 88                    |


================


Select   F_NAME 
From <%=odiRef.getObjectName("L","FILE_NAME","D")%>
SELECT F_NAME FROM <?=odiRef.getObjectName("L", "FILE_NAME", "ScottLS", "", "D") ?> 




















