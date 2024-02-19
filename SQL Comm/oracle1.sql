Path for tnsnames.ora,listerner.ora,sqlnet.ora
==============================================
Oracle Path
C:\app
G:\app\vijay\product\19.0.0\client_1
G:\app\vijay\product\19.0.0\client_1
C:\db_home\bin

C:\Users\vijay\Oracle\network\admin
C:\Users\vijay\Oracle\network
C:\db_home\network\admin
C:\Program Files\Oracle Client for Microsoft Tools

==============================================
select * from v$version
select distinct address from v$sql where rownum<2;
select * from global_name;
select name,open_mode from V$DATABASE;

select USER from dual;
create user scott identified by tiger;

ALTER USER system IDENTIFIED BY system ACCOUNT UNLOCK;
ALTER USER scott IDENTIFIED BY tiger ACCOUNT UNLOCK;
alter user scott account unlock identified by tiger;
-----------------------------------------------------


select username from all_users;
GRANT CONNECT, RESOURCE, DBA TO books_admin;
GRANT DBA TO scott;
GRANT DBA TO scott WITH ADMIN OPTION;
grant all privileges to scott; --Main
grant connect to scott;
-----------------------------------------------------

create user mrepo11 identified by mrepo11;
alter user mrepo11 account unlock identified by mrepo11;

create user scott identified by tiger;
alter user scott account unlock identified by tiger1;

show con_name;
alter session set container=orclpdb;
select name ,open_mode from V$pdbs;
SELECT name || ' '|| pdb FROM v$services ORDER BY name;

Alter pluggable database all open;

connect sys/sysdba@orclpdb as sysdba

 
 ============
SELECT NAME, LOG_MODE, OPEN_MODE, DATABASE_ROLE, PLATFORM_NAME FROM  v$database;
SELECT *FROM v$version;

SELECT SUM(BYTES)/1024/1024/1024 AS “DBSIZE(GB)” FROM dba_data_files;
SELECT DISTINCT client_version FROM v$session_connect_info WHERE sid = sys_context(‘userenv’, ‘sid’);
SELECT INSTANCE_NAME, HOST_NAME, VERSION, STARTUP_TIME, STATUS, INSTANCE_ROLE FROM v$instance;
SELECT OWNER FROM DBA_TABLES WHERE TABLE_NAME = ‘T000’;
SELECT * FROM SAPSR3.TSTC;
select distinct datum,uname,mandt from sapsr3.snap where datum='20220715';


=============================================================================================

Create user SCOTT identified by tiger;
GRANT DBA TO SCOTT;
GRANT DBA TO SCOTT WITH ADMIN OPTION;
grant all privileges to SCOTT;


Create user Mrepo identified by mrepo;
GRANT DBA TO Mrepo;
GRANT DBA TO Mrepo WITH ADMIN OPTION;
grant all privileges to Mrepo;

Create user Wrepo identified by wrepo;
GRANT DBA TO Wrepo;
GRANT DBA TO Wrepo WITH ADMIN OPTION;
grant all privileges to Wrepo;

Create user ProdWrepo identified by prodwrepo;
GRANT DBA TO ProdWrepo;
GRANT DBA TO ProdWrepo WITH ADMIN OPTION;
grant all privileges to ProdWrepo;

select name,open_mode from V$DATABASE;

Create user Staging identified by staging;
GRANT DBA TO Staging;
GRANT DBA TO Staging WITH ADMIN OPTION;
grant all privileges to Staging;

Create user Tgtdb identified by tgtdb;
GRANT DBA TO Tgtdb;
GRANT DBA TO Tgtdb WITH ADMIN OPTION;
grant all privileges to Tgtdb;

============================
I also faced the same issue ORA-12543: TNS:destination host unreachable
i resolved it in this way

open sqlplus
connect

enter user-name :system enter password : HHHHH@2014

then the following error raised
the problem is my password contains @ symbol

resolved it by putting my password in "HHHHHH@2014"
==========================================================================
create table dept(
  deptno number(2,0),
  dname  varchar2(14),
  loc    varchar2(13),
  constraint pk_dept primary key (deptno)
);
 
create table emp(
  empno    number(4,0),
  ename    varchar2(10),
  job      varchar2(9),
  mgr      number(4,0),
  hiredate date,
  sal      number(7,2),
  comm     number(7,2),
  deptno   number(2,0),
  constraint pk_emp primary key (empno),
  constraint fk_deptno foreign key (deptno) references dept (deptno)
);
 
/*
create table bonus(
  ename varchar2(10),
  job   varchar2(9),
  sal   number,
  comm  number
);
 
create table salgrade(
  grade number,
  losal number,
  hisal number
);
*/
------------
insert into dept
values(10, 'ACCOUNTING', 'NEW YORK');
insert into dept
values(20, 'RESEARCH', 'DALLAS');
insert into dept
values(30, 'SALES', 'CHICAGO');
insert into dept
values(40, 'OPERATIONS', 'BOSTON');
 
insert into emp
values(
 7839, 'KING', 'PRESIDENT', null,
 to_date('17-11-1981','dd-mm-yyyy'),
 5000, null, 10
);
insert into emp
values(
 7698, 'BLAKE', 'MANAGER', 7839,
 to_date('1-5-1981','dd-mm-yyyy'),
 2850, null, 30
);
insert into emp
values(
 7782, 'CLARK', 'MANAGER', 7839,
 to_date('9-6-1981','dd-mm-yyyy'),
 2450, null, 10
);
insert into emp
values(
 7566, 'JONES', 'MANAGER', 7839,
 to_date('2-4-1981','dd-mm-yyyy'),
 2975, null, 20
);
insert into emp
values(
 7788, 'SCOTT', 'ANALYST', 7566,
 to_date('13-JUL-87','dd-mm-rr') - 85,
 3000, null, 20
);
insert into emp
values(
 7902, 'FORD', 'ANALYST', 7566,
 to_date('3-12-1981','dd-mm-yyyy'),
 3000, null, 20
);
insert into emp
values(
 7369, 'SMITH', 'CLERK', 7902,
 to_date('17-12-1980','dd-mm-yyyy'),
 800, null, 20
);
insert into emp
values(
 7499, 'ALLEN', 'SALESMAN', 7698,
 to_date('20-2-1981','dd-mm-yyyy'),
 1600, 300, 30
);
insert into emp
values(
 7521, 'WARD', 'SALESMAN', 7698,
 to_date('22-2-1981','dd-mm-yyyy'),
 1250, 500, 30
);
insert into emp
values(
 7654, 'MARTIN', 'SALESMAN', 7698,
 to_date('28-9-1981','dd-mm-yyyy'),
 1250, 1400, 30
);
insert into emp
values(
 7844, 'TURNER', 'SALESMAN', 7698,
 to_date('8-9-1981','dd-mm-yyyy'),
 1500, 0, 30
);
insert into emp
values(
 7876, 'ADAMS', 'CLERK', 7788,
 to_date('13-JUL-87', 'dd-mm-rr') - 51,
 1100, null, 20
);
insert into emp
values(
 7900, 'JAMES', 'CLERK', 7698,
 to_date('3-12-1981','dd-mm-yyyy'),
 950, null, 30
);
insert into emp
values(
 7934, 'MILLER', 'CLERK', 7782,
 to_date('23-1-1982','dd-mm-yyyy'),
 1300, null, 10
);
 
/*
insert into salgrade
values (1, 700, 1200);
insert into salgrade
values (2, 1201, 1400);
insert into salgrade
values (3, 1401, 2000);
insert into salgrade
values (4, 2001, 3000);
insert into salgrade
values (5, 3001, 9999);
*/
 
 
 
 ====================
 
 
 Java Installation
-----------------
C:\Java\jdk1.8.0_351\

C:\Java\jre1.8.0_351\

=======================================
Oracle base:-C:\app\vijay
s/w location :-C:\db_home
db file location:-C:\app\vijay\oradata
Global db name:-orcl1
pwd :-Vijay@123 confirm pwd :-Vijay@123

Oracle Enterprise Manager Database Express URL: https://localhost:5500/em
========================================

WINDOWS.X64_193000_db_home

C:\db_home\jdk\jre\bin\java.exe

:\db_home\bin\lsnrctl.exe start LISTENER

Oracle Enterprise Manager Database Express URL: https://localhost:5500/em