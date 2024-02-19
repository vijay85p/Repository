select * from emp;
INSERT INTO sh.cust_top_ten
            (sales_rank,
             cust_first_name,
             cust_id,
             sales_amt,
             cust_last_name)
SELECT sales_rank,
       cust_first_name,
       cust_id,
       sales_amt,
       cust_last_name
FROM   (SELECT sales_rank.sales_rank     sales_rank,
               customers.cust_first_name cust_first_name,
               customers.cust_id         cust_id,
               sales_rank.sales_amt      sales_amt,
               customers.cust_last_name  cust_last_name
        FROM   (SELECT sales_cust.cust_id  cust_id,
                       sales_cust.sales_amt sales_amt,
                       Dense_rank() over (ORDER BY sales_cust.sales_amt DESC) sales_rank
                FROM   (SELECT sales.cust_id  cust_id,
                               SUM (sales.amount_sold) sales_amt  FROM   sh.sales sales
                        WHERE  ( 1 = 1 ) GROUP  BY sales.cust_id) sales_cust WHERE  ( 1 = 1 )) sales_rank,
               sh.customers customers
        WHERE  ( 1 = 1 )
               AND ( sales_rank.cust_id = customers.cust_id )
               AND ( sales_rank.sales_rank &lt;= 10 )) odi_get_from
			   
==============
Merger Statement 


MERGE INTO employees_9 e
USING employees_updates eu
ON (e.employee_id = eu.employee_id)
WHEN MATCHED THEN
    UPDATE SET e.salary = eu.new_salary
WHEN NOT MATCHED THEN
    INSERT   (e.employee_id, e.employee_name, e.salary)
    VALUES (eu.employee_id, 'New Employee', eu.new_salary);

Inline Query in WHERE Clause
============================
SELECT * FROM employees
WHERE department_id IN (SELECT department_id FROM departments WHERE department_name = 'Sales');

In this example, the inline query (SELECT department_id FROM departments WHERE department_name = 'Sales') retrieves the department_id from the departments 
table, and the outer query fetches all columns from the employees table 
where the department_id is in the set of IDs retrieved from the inner query.

Inline Query in SELECT Clause
=============================
SELECT employee_name, 
       (SELECT MAX(salary) FROM employees WHERE department_id = 1) AS max_salary
FROM employees;

Here, the inline query (SELECT MAX(salary) FROM employees WHERE department_id = 1) retrieves the maximum salary from the employees 
table for the department with department_id = 1. The outer query selects employee_name from the employees table 
and includes the maximum salary as max_salary for all employees.

Inline Query in FROM Clause (Inline View)
=========================================
SELECT e.employee_id, e.employee_name, d.department_name
FROM employees e
JOIN (SELECT department_id, department_name FROM departments WHERE location = 'New York') d
ON e.department_id = d.department_id;

In this example, (SELECT department_id, department_name FROM departments WHERE location = 'New York') 
creates an inline view d that retrieves specific columns from the departments table. 
The outer query joins this inline view with the employees table on the department_id.

Inline queries allow for more dynamic and complex SQL operations by leveraging nested queries within the main query structure, 
enabling conditions, aggregations, or selections based on the results of the inner query.

correlated subquery
===================
SELECT employee_id, employee_name, salary, department_id
FROM employees e
WHERE salary > (
    SELECT AVG(salary)
    FROM salaries s
    WHERE s.department_id = e.department_id
);
The outer query selects employee_id, employee_name, salary, and department_id from the employees table (e alias).
The correlated subquery (SELECT AVG(salary) FROM salaries s WHERE s.department_id = e.department_id) calculates the average salary for each employees 
department in the salaries table (s alias). The correlation occurs via s.department_id = e.department_id, linking the subquery to the outer query.

The WHERE clause in the outer query filters employees where their salary is greater than the average salary of their respective department.
In this example, the correlated subquery dynamically calculates the average salary for each employees department, allowing the main query 
to filter employees based on this department-specific average salary.

Correlated subqueries are powerful but can impact performance since they execute once per row in the outer query. 

Its important to use them judiciously and optimize queries where possible.

=======

In Oracle, both views and materialized views are database objects used for storing queries. However, they differ in their structure, storage, and behavior.

Views:
A view in Oracle is a virtual table that is based on the result set of a SQL query. It does not store data on its own but rather provides a way to present data from one or more tables or views in a predefined manner.

Creating a View:
CREATE VIEW employee_view AS
SELECT employee_id, employee_name, department_id
FROM employees
WHERE department_id = 1;

Materialized Views:
A materialized view is a physical copy or snapshot of the result set of a query that is stored in the database. 
Unlike views, materialized views persist the data, allowing for faster query performance and reduced workload
on the source tables by precomputing and storing the result set.

CREATE MATERIALIZED VIEW mv_employee_department AS
SELECT department_id, AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id;

This creates a materialized view named mv_employee_department that stores the average salary for each department from the employees table.

Refreshing a Materialized View:
Materialized views need to be refreshed to ensure their data is up-to-date. 
This can be done manually or automatically based on a defined schedule or upon specific events.
-- Manually refresh a materialized view
EXEC DBMS_MVIEW.REFRESH('mv_employee_department');

CREATE OR REPLACE 
PROCEDURE MAT_VIEW_FOO_TBL 
IS
BEGIN
   DBMS_MVIEW.REFRESH('v_materialized_foo_tbl')
END MAT_VIEW_FOO_TBL IS;


-- Automatically refresh a materialized view every day at 2 AM
CREATE MATERIALIZED VIEW mv_employee_department
REFRESH FAST START WITH SYSDATE+1 NEXT SYSDATE+1/24
AS
SELECT department_id, AVG(salary) AS avg_salary
FROM employees
GROUP BY department_id;


SELECT e.employee_id, e.employee_name, d.department_name, l.city
FROM employees e
LEFT JOIN departments d ON e.department_id = d.department_id
LEFT JOIN locations l ON d.location_id = l.location_id;


-=================-
In Oracle, synonyms are database objects that serve as aliases or alternate names for other objects, 
such as tables, views, sequences, procedures, or other synonyms. They allow users to reference objects in different schemas without specifying the schema name each time,
simplifying access and providing a level of abstraction.

SELECT * FROM ALL_SYNONYMS WHERE synonym_name = 'emp';

To_date & TO_CHAR
=================
Select SYSDATE,to_date('15-aug-1947') ,round(sysdate-to_date('15-aug-1947'))from dual;
Select floor(months_between(sysdate,'06-nov-1985'))/12 age_in_months from dual;
Select  next_day (sysdate,'SATURDAY' ) from dual; Select to_char(Sysdate,'dd-mon-yyyy') from dual;
select add_months(sysdate,-3) from dual;
Select job from emp where deptno=10 and  job in(select job from emp where deptno=20);
Select job from emp where deptno=10 minus select job from emp where deptno!=10;
select e.empno,e.ename,m.empno manager,m.ename managername from emp e, emp m where e.mgr=m.empno and e.deptno=10;
select * from emp e where hiredate=(select max(hiredate) from emp where deptno=e.deptno) order by hiredate;
select length(ename),concat(upper(substr(ename,0,length(ename)/2)),'_p'),
lower(substr(ename,length(ename)/2+1,length(ename)))  from emp;

SELECT s1.number_value, s2.letter_value
FROM (
    SELECT LEVEL AS number_value FROM dual CONNECT BY LEVEL <= 5 -- Generating numbers 1 to 5
) s1
CROSS JOIN (
    SELECT  CHR(64 + LEVEL) AS letter_value FROM dual CONNECT BY LEVEL <= 5 -- Generating letters 'A' to 'E'
) s2;



==================
DBMS_STATS

-- Connect to your schema
ALTER SESSION SET CURRENT_SCHEMA = sales;

-- Gather statistics for the 'sales_data' table
BEGIN
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname         => 'sales',       -- Schema name
    tabname         => 'sales_data',  -- Table name
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    method_opt      => 'FOR ALL COLUMNS SIZE AUTO',
    CASCADE         => TRUE
  );
END;
/

The output of the DBMS_STATS.GATHER_TABLE_STATS procedure isnt directly visible in the form of a result set like a regular SQL query. Instead, the statistics gathered by this procedure are stored in the Oracle data dictionary tables.

You can query these data dictionary views to view the gathered statistics. 
Some of the commonly used views to check the statistics include:

USER_TAB_STATISTICS: If youre the owner of the table, you can query this view to see statistics for your tables.
ALL_TAB_STATISTICS: If you have access to view statistics across all tables in your Oracle database.
DBA_TAB_STATISTICS: This view allows DBAs to view statistics for all tables in the database.
For example, to view statistics for the 'sales_data' table in the 'sales' schema, you can use a query like this.

SELECT * FROM USER_TAB_STATISTICS WHERE TABLE_NAME = 'SALES_DATA';
