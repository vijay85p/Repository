Installation of oracle 12c on windows 10
========================================
https://www.oracle.com/in/java/technologies/downloads/#jre8-windows

https://www.youtube.com/@Rebellionrider/playlists
container db orcl/oracle
pluggable database orclpdb

commands
========
https://logic.edchen.org/how-to-resolve-ora-65096-invalid-common-user-or-role-name/
show con_name

https://ohiocomputeracademy.com/database/unlocking-scott-schema-in-oracle/#:~:text=Modifying%20scott.&text=If%20you%20have%20followed%20the,%E2%80%9CCONNECT%20SCOTT%2FTIGER%E2%80%9D.&text=Now%20copy%20and%20paste%20CONNECT,CONNECT%20SCOTT%2FTIGER%40orclpdb.
https://www.youtube.com/watch?v=sjj4GjqvwK4&t=153s

=====================

odi 11g installation
====================
Oracle Home Directory
C:\oracle\product\11.1.1\Oracle_ODI_1

Agent Name :-OracleDIAgent
Agent Port :-20910
G:\oracle\product\11.1.1\Oracle_ODI_1\oracledi\client

Master_Repo_Login
create user C##Mrepo IDENTIFIED by mrepo;
create user C##Wrepo IDENTIFIED by wrepo;

GRANT DBA TO C##Mrepo;
GRANT DBA TO C##Mrepo WITH ADMIN OPTION;
grant all privileges to C##Mrepo;

GRANT DBA TO C##Wrepo;
GRANT DBA TO C##Wrepo WITH ADMIN OPTION;
grant all privileges to C##Wrepo;


Fresh Installation(13/01/2023)
------------------
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

drop user Wrepo cascade;

Create user ProdWrepo identified by prodwrepo;
GRANT DBA TO ProdWrepo;
GRANT DBA TO ProdWrepo WITH ADMIN OPTION;
grant all privileges to ProdWrepo;

C:\Java\jdk1.8.0_351\bin
Oracle Home Directory C:\oracle\product\11.1.1\Oracle_ODI_1

[7:00:48 PM] Applying DDL from file C:\oracle\product\11.1.1\Oracle_ODI_1\oracledi.sdk\lib\scripts\ORACLE\W_DROP.xml
[7:00:49 PM] Applying DDL from file C:\oracle\product\11.1.1\Oracle_ODI_1\oracledi.sdk\lib\scripts\ORACLE\E_DROP.xml
[7:00:50 PM] Applying DDL from file C:\oracle\product\11.1.1\Oracle_ODI_1\oracledi.sdk\lib\scripts\ORACLE\E_CREATE.xml
[7:00:54 PM] Applying DDL from file C:\oracle\product\11.1.1\Oracle_ODI_1\oracledi.sdk\lib\scripts\ORACLE\W_CREATE.xml
[7:01:00 PM] Work repository creation is successful.
[7:13:01 PM] Applying DDL from file C:\oracle\product\11.1.1\Oracle_ODI_1\oracledi.sdk\lib\scripts\ORACLE\E_DROP.xml
[7:13:02 PM] Applying DDL from file C:\oracle\product\11.1.1\Oracle_ODI_1\oracledi.sdk\lib\scripts\ORACLE\E_CREATE.xml
[7:13:05 PM] Work repository creation is successful