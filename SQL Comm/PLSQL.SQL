PL/SQL
======
A subprogram is a program unit/module that performs a particular task. These subprograms are combined to form larger programs. 
This is basically called the 'Modular design'. 
A subprogram can be invoked by another subprogram or program which is called the calling program.

PL/SQL subprograms are named PL/SQL blocks that can be invoked with a set of parameters. PL/SQL provides two kinds of subprograms.

1. Functions − These subprograms return a single value; mainly used to compute and return a value.
2. Procedures − These subprograms do not return a value directly; mainly used to perform an action.

Like anonymous PL/SQL blocks, the named blocks will also have the following three parts −

Creating a Procedure
A procedure is created with the CREATE OR REPLACE PROCEDURE statement. 
The simplified syntax for the CREATE OR REPLACE PROCEDURE statement is as follows −
	CREATE [OR REPLACE] PROCEDURE procedure_name 
	[(parameter_name [IN | OUT | IN OUT] type [, ...])] 
	{IS | AS} 
	BEGIN 
	< procedure_body > 
	END procedure_name; 
	
The following example creates a simple procedure that displays the string 'Hello World!' on the screen when executed.

CREATE OR REPLACE PROCEDURE greetings 
AS 
BEGIN 
   dbms_output.put_line('Hello World!'); 
END; 
/
The above procedure named 'greetings' can be called with the EXECUTE keyword as −
EXECUTE greetings;

The procedure can also be called from another PL/SQL block −
BEGIN 
   greetings; 
END; 
/

IN & OUT Mode Example 1
=======================

This program finds the minimum of two values. Here, the procedure takes two numbers using the IN mode and returns their minimum using the OUT parameters.

DECLARE 
   a number; 
   b number; 
   c number;
PROCEDURE findMin(x IN number, y IN number, z OUT number) IS 
BEGIN 
   IF x < y THEN 
      z:= x; 
   ELSE 
      z:= y; 
   END IF; 
END;   
BEGIN 
   a:= 23; 
   b:= 45; 
   findMin(a, b, c); 
   dbms_output.put_line(' Minimum of (23, 45) : ' || c); 
END; 
/
When the above code is executed at the SQL prompt, it produces the following result −

Minimum of (23, 45) : 23  

PL/SQL procedure successfully completed. 

IN & OUT Mode Example 2
=======================

This procedure computes the square of value of a passed value. This example shows how we can use the same parameter to accept a value and then return another result.

DECLARE 
   a number; 
PROCEDURE squareNum(x IN OUT number) IS 
BEGIN 
  x := x * x; 
END;  
BEGIN 
   a:= 23; 
   squareNum(a); 
   dbms_output.put_line(' Square of (23): ' || a); 
END; 
/

=======================================================================================
Function 
A function is same as a procedure except that it returns a value. 
Therefore, all the discussions of the previous chapter are true for functions too.

Creating a Function
A standalone function is created using the CREATE FUNCTION statement. The simplified syntax for the CREATE OR REPLACE PROCEDURE statement is as follows −

CREATE [OR REPLACE] FUNCTION function_name 
[(parameter_name [IN | OUT | IN OUT] type [, ...])] 
RETURN return_datatype 
{IS | AS} 
BEGIN 
   < function_body > 
END [function_name];

CREATE OR REPLACE FUNCTION totalCustomers 
RETURN number IS 
   total number(2) := 0; 
BEGIN 
   SELECT count(*) into total 
   FROM customers; 
    
   RETURN total; 
END; 
/ 
While creating a function, you give a definition of what the function has to do. 
To use a function, you will have to call that function to perform the defined task. 
When a program calls a function, the program control is transferred to the called function.

A called function performs the defined task and when its return statement is executed or when the last end statement is reached, 
it returns the program control back to the main program.

To call a function, you simply need to pass the required parameters along with the function name and if the function returns a value, 
then you can store the returned value. Following program calls the function totalCustomers from an anonymous block −

DECLARE 
   c number(2); 
BEGIN 
   c := totalCustomers(); 
   dbms_output.put_line('Total no. of Customers: ' || c); 
END; 
/

Example
The following example demonstrates Declaring, Defining, and Invoking a Simple PL/SQL Function that computes and returns the maximum of two values.

DECLARE 
   a number; 
   b number; 
   c number; 
FUNCTION findMax(x IN number, y IN number)  
RETURN number 
IS 
    z number; 
BEGIN 
   IF x > y THEN 
      z:= x; 
   ELSE 
      Z:= y; 
   END IF;  
   RETURN z; 
END; 
BEGIN 
   a:= 23; 
   b:= 45;  
   c := findMax(a, b); 
   dbms_output.put_line(' Maximum of (23,45): ' || c); 
END; 
/ 
When the above code is executed at the SQL prompt, it produces the following result −

Maximum of (23,45): 45   

PL/SQL procedure successfully completed. 

===========

PL/SQL Recursive Functions
We have seen that a program or subprogram may call another subprogram. When a subprogram calls itself, it is referred to as a recursive call and the process is known as recursion.

To illustrate the concept, let us calculate the factorial of a number. Factorial of a number n is defined as −

n! = n*(n-1)! 
   = n*(n-1)*(n-2)! 
      ... 
   = n*(n-1)*(n-2)*(n-3)... 1 
   
==========
DECLARE 
   num number; 
   factorial number;  
   
FUNCTION fact(x number) 
RETURN number  
IS 
   f number; 
BEGIN 
   IF x=0 THEN 
      f := 1; 
   ELSE 
      f := x * fact(x-1); 
   END IF; 
RETURN f; 
END;  

BEGIN 
   num:= 6; 
   factorial := fact(num); 
   dbms_output.put_line(' Factorial '|| num || ' is ' || factorial); 
END; 
/   

Factorial 6 is 720 
  
PL/SQL procedure successfully completed.

=================

PL/SQL - Triggers
=================

Triggers are stored programs, which are automatically executed or fired when some events occur. Triggers are, 
in fact, written to be executed in response to any of the following events −

A database manipulation (DML) statement (DELETE, INSERT, or UPDATE)
A database definition (DDL) statement (CREATE, ALTER, or DROP).
A database operation (SERVERERROR, LOGON, LOGOFF, STARTUP, or SHUTDOWN).
Triggers can be defined on the table, view, schema, or database with which the event is associated.

Benefits of Triggers
Triggers can be written for the following purposes −
1. Enforcing referential integrity
2. Event logging and storing information on table access Auditing
3. Synchronous replication of tables
4. Imposing security authorizations
5. Preventing invalid transactions

Creating Triggers
The syntax for creating a trigger is −

CREATE [OR REPLACE ] TRIGGER trigger_name  
{BEFORE | AFTER | INSTEAD OF }  
{INSERT [OR] | UPDATE [OR] | DELETE}  
[OF col_name]  
ON table_name  
[REFERENCING OLD AS o NEW AS n]  
[FOR EACH ROW]  
WHEN (condition)   
DECLARE 
   Declaration-statements 
BEGIN  
   Executable-statements 
EXCEPTION 
   Exception-handling-statements 
END; 

Triggering a Trigger
Let us perform some DML operations on the CUSTOMERS table. Here is one INSERT statement, which will create a new record in the table −

INSERT INTO CUSTOMERS (ID,NAME,AGE,ADDRESS,SALARY) 
VALUES (7, 'Kriti', 22, 'HP', 7500.00 ); 
When a record is created in the CUSTOMERS table, the above create trigger, display_salary_changes will be fired and it will display the following result −

Old salary: 
New salary: 7500 
Salary difference:
Because this is a new record, old salary is not available and the above result comes as null. Let us now perform one more DML operation on the CUSTOMERS table. The UPDATE statement will update an existing record in the table −

UPDATE customers 
SET salary = salary + 500 
WHERE id = 2; 
When a record is updated in the CUSTOMERS table, the above create trigger, display_salary_changes will be fired and it will display the following result −

Old salary: 1500 
New salary: 2000 
Salary difference: 500 

PL/SQL - Cursors
================

Oracle creates a memory area, known as the context area, for processing an SQL statement, 
which contains all the information needed for processing the statement; for example, the number of rows processed, etc.

A cursor is a pointer to this context area. PL/SQL controls the context area through a cursor. 
A cursor holds the rows (one or more) returned by a SQL statement. The set of rows the cursor holds is referred to as the active set.

You can name a cursor so that it could be referred to in a program to fetch and process the rows returned by the SQL statement, 
one at a time. 

There are two types of cursors −

Implicit cursors
Explicit cursors

Implicit Cursors
Implicit cursors are automatically created by Oracle whenever an SQL statement is executed, 
when there is no explicit cursor for the statement. Programmers cannot control the implicit cursors and the information in it.

Whenever a DML statement (INSERT, UPDATE and DELETE) is issued, an implicit cursor is associated with this statement. 
For INSERT operations, the cursor holds the data that needs to be inserted. For UPDATE and DELETE operations, 
the cursor identifies the rows that would be affected.

In PL/SQL, you can refer to the most recent implicit cursor as the SQL cursor, 
which always has attributes such as %FOUND, %ISOPEN, %NOTFOUND, and %ROWCOUNT. 
The SQL cursor has additional attributes, %BULK_ROWCOUNT and %BULK_EXCEPTIONS, designed for use with the FORALL statement. 
The following table provides the description of the most used attributes −


1. %FOUND

Returns TRUE if an INSERT, UPDATE, or DELETE statement affected one or more rows or a SELECT INTO statement returned one or more rows. 
Otherwise, it returns FALSE.

2. %NOTFOUND

The logical opposite of %FOUND. It returns TRUE if an INSERT, UPDATE, or DELETE statement affected no rows, or a SELECT INTO statement returned no rows. Otherwise, it returns FALSE.

3. %ISOPEN

Always returns FALSE for implicit cursors, because Oracle closes the SQL cursor automatically after executing its associated SQL statement.

4. %ROWCOUNT

Returns the number of rows affected by an INSERT, UPDATE, or DELETE statement, or returned by a SELECT INTO statement.

Any SQL cursor attribute will be accessed as sql%attribute_name as shown below in the example.
The following program will update the table and increase the salary of each customer by 500 and use the SQL%ROWCOUNT attribute to determine the number of rows affected −

DECLARE  
   total_rows number(2); 
BEGIN 
   UPDATE customers 
   SET salary = salary + 500; 
   IF sql%notfound THEN 
      dbms_output.put_line('no customers selected'); 
   ELSIF sql%found THEN 
      total_rows := sql%rowcount;
      dbms_output.put_line( total_rows || ' customers selected '); 
   END IF;  
END; 
/      

Explicit Cursors
================

Explicit cursors are programmer-defined cursors for gaining more control over the context area. An explicit cursor should be defined in the declaration section of the PL/SQL Block. It is created on a SELECT Statement which returns more than one row.

The syntax for creating an explicit cursor is −

CURSOR cursor_name IS select_statement; 
Working with an explicit cursor includes the following steps −

Declaring the cursor for initializing the memory
Opening the cursor for allocating the memory
Fetching the cursor for retrieving the data
Closing the cursor to release the allocated memory

Declaring the Cursor
Declaring the cursor defines the cursor with a name and the associated SELECT statement. For example −

CURSOR c_customers IS 
   SELECT id, name, address FROM customers; 

Opening the Cursor
Opening the cursor allocates the memory for the cursor and makes it ready for fetching the rows returned by the SQL statement into it. For example, we will open the above defined cursor as follows −

OPEN c_customers; 

Fetching the Cursor
Fetching the cursor involves accessing one row at a time. For example, we will fetch rows from the above-opened cursor as follows −

FETCH c_customers INTO c_id, c_name, c_addr; 
Closing the Cursor
Closing the cursor means releasing the allocated memory. For example, we will close the above-opened cursor as follows −

CLOSE c_customers;

Example
Following is a complete example to illustrate the concepts of explicit cursors &minua;

DECLARE 
   c_id customers.id%type; 
   c_name customers.name%type; 
   c_addr customers.address%type; 
   CURSOR c_customers is 
      SELECT id, name, address FROM customers; 
BEGIN 
   OPEN c_customers; 
   LOOP 
   FETCH c_customers into c_id, c_name, c_addr; 
      EXIT WHEN c_customers%notfound; 
      dbms_output.put_line(c_id || ' ' || c_name || ' ' || c_addr); 
   END LOOP; 
   CLOSE c_customers; 
END; 
/
When the above code is executed at the SQL prompt, it produces the following result −

1 Ramesh Ahmedabad  
2 Khilan Delhi  
3 kaushik Kota     
4 Chaitali Mumbai  
5 Hardik Bhopal   
6 Komal MP  
  
PL/SQL procedure successfully completed. 

PL/SQL - Exceptions
===================

An exception is an error condition during a program execution. PL/SQL supports programmers to catch such conditions using EXCEPTION block in
the program and an appropriate action is taken against the error condition. There are two types of exceptions −   

System-defined exceptions
User-defined exceptions

Syntax for Exception Handling
The general syntax for exception handling is as follows. Here you can list down as many exceptions as you can handle. The default exception will be handled using WHEN others THEN −

DECLARE 
   <declarations section> 
BEGIN 
   <executable command(s)> 
EXCEPTION 
   <exception handling goes here > 
   WHEN exception1 THEN  
      exception1-handling-statements  
   WHEN exception2  THEN  
      exception2-handling-statements  
   WHEN exception3 THEN  
      exception3-handling-statements 
   ........ 
   WHEN others THEN 
      exception3-handling-statements 
END;

Example
Let us write a code to illustrate the concept. We will be using the CUSTOMERS table we had created and used in the previous chapters −

DECLARE 
   c_id customers.id%type := 8; 
   c_name customerS.Name%type; 
   c_addr customers.address%type; 
BEGIN 
   SELECT  name, address INTO  c_name, c_addr 
   FROM customers 
   WHERE id = c_id;  
   DBMS_OUTPUT.PUT_LINE ('Name: '||  c_name); 
   DBMS_OUTPUT.PUT_LINE ('Address: ' || c_addr); 

EXCEPTION 
   WHEN no_data_found THEN 
      dbms_output.put_line('No such customer!'); 
   WHEN others THEN 
      dbms_output.put_line('Error!'); 
END; 
/
When the above code is executed at the SQL prompt, it produces the following result −

No such customer!  

PL/SQL procedure successfully completed. 
Raising Exceptions
Exceptions are raised by the database server automatically whenever there is any internal database error, 
but exceptions can be raised explicitly by the programmer by using the command RAISE. 
Following is the simple syntax for raising an exception −

DECLARE 
   exception_name EXCEPTION; 
BEGIN 
   IF condition THEN 
      RAISE exception_name; 
   END IF; 
EXCEPTION 
   WHEN exception_name THEN 
   statement; 
END; 

User-defined Exceptions
PL/SQL allows you to define your own exceptions according to the need of your program. 
A user-defined exception must be declared and then raised explicitly, using either a RAISE statement or the 
procedure DBMS_STANDARD.RAISE_APPLICATION_ERROR.

Example
The following example illustrates the concept. This program asks for a customer ID, when the user enters an invalid ID, the exception invalid_id is raised.

DECLARE 
   c_id customers.id%type := &cc_id; 
   c_name customerS.Name%type; 
   c_addr customers.address%type;  
   -- user defined exception 
   ex_invalid_id  EXCEPTION; 
BEGIN 
   IF c_id <= 0 THEN 
      RAISE ex_invalid_id; 
   ELSE 
      SELECT  name, address INTO  c_name, c_addr 
      FROM customers 
      WHERE id = c_id;
      DBMS_OUTPUT.PUT_LINE ('Name: '||  c_name);  
      DBMS_OUTPUT.PUT_LINE ('Address: ' || c_addr); 
   END IF; 

EXCEPTION 
   WHEN ex_invalid_id THEN 
      dbms_output.put_line('ID must be greater than zero!'); 
   WHEN no_data_found THEN 
      dbms_output.put_line('No such customer!'); 
   WHEN others THEN 
      dbms_output.put_line('Error!');  
END; 
/

re-defined Exceptions
PL/SQL provides many pre-defined exceptions, which are executed when any database rule is violated by a program. For example, the predefined exception NO_DATA_FOUND is raised when a SELECT INTO statement returns no rows. The following table lists few of the important pre-defined exceptions −

Exception		Oracle Error	SQLCODE	Description
ACCESS_INTO_NULL	06530		-6530	It is raised when a null object is automatically assigned a value.
CASE_NOT_FOUND		06592		-6592	It is raised when none of the choices in the WHEN clause of a CASE statement is selected, and there is no ELSE clause.
COLLECTION_IS_NULL	06531		-6531	It is raised when a program attempts to apply collection methods other than EXISTS to an uninitialized nested table or varray, or the program attempts to assign values to the elements of an uninitialized nested table or varray.
DUP_VAL_ON_INDEX	00001		-1	It is raised when duplicate values are attempted to be stored in a column with unique index.
INVALID_CURSOR		01001		-1001	It is raised when attempts are made to make a cursor operation that is not allowed, such as closing an unopened cursor.
INVALID_NUMBER		01722		-1722	It is raised when the conversion of a character string into a number fails because the string does not represent a valid number.
LOGIN_DENIED		01017		-1017	It is raised when a program attempts to log on to the database with an invalid username or password.
NO_DATA_FOUND		01403		+100	It is raised when a SELECT INTO statement returns no rows.
NOT_LOGGED_ON		01012		-1012	It is raised when a database call is issued without being connected to the database.
PROGRAM_ERROR		06501		-6501	It is raised when PL/SQL has an internal problem.
ROWTYPE_MISMATCH	06504		-6504	It is raised when a cursor fetches value in a variable having incompatible data type.
SELF_IS_NULL		30625		-30625	It is raised when a member method is invoked, but the instance of the object type was not initialized.
STORAGE_ERROR		06500		-6500	It is raised when PL/SQL ran out of memory or memory was corrupted.
TOO_MANY_ROWS		01422		-1422	It is raised when a SELECT INTO statement returns more than one row.
VALUE_ERROR			06502		-6502	It is raised when an arithmetic, conversion, truncation, or sizeconstraint error occurs.
ZERO_DIVIDE			01476		1476	It is raised when an attempt is made to divide a number by zero.


PL/SQL - Records
================

A record is a data structure that can hold data items of different kinds. Records consist of different fields, similar to a row of a database table.

For example, you want to keep track of your books in a library. You might want to track the following attributes about each book, such as Title, Author, Subject, Book ID. A record containing a field for each of these items allows treating a BOOK as a logical unit and allows you to organize and represent its information in a better way.

PL/SQL can handle the following types of records −

Table-based
Cursor-based records
User-defined records

Table-Based Records
The %ROWTYPE attribute enables a programmer to create table-based and cursorbased records.

The following example illustrates the concept of table-based records. We will be using the CUSTOMERS table we had created and used in the previous chapters −

DECLARE 
   customer_rec customers%rowtype; 
BEGIN 
   SELECT * into customer_rec 
   FROM customers 
   WHERE id = 5;  
   dbms_output.put_line('Customer ID: ' || customer_rec.id); 
   dbms_output.put_line('Customer Name: ' || customer_rec.name); 
   dbms_output.put_line('Customer Address: ' || customer_rec.address); 
   dbms_output.put_line('Customer Salary: ' || customer_rec.salary); 
END; 
/
When the above code is executed at the SQL prompt, it produces the following result −

Customer ID: 5 
Customer Name: Hardik 
Customer Address: Bhopal 
Customer Salary: 9000 
 
PL/SQL procedure successfully completed.
Cursor-Based Records
The following example illustrates the concept of cursor-based records. We will be using the CUSTOMERS table we had created and used in the previous chapters −

DECLARE 
   CURSOR customer_cur is 
      SELECT id, name, address  
      FROM customers; 
   customer_rec customer_cur%rowtype; 
BEGIN 
   OPEN customer_cur; 
   LOOP 
      FETCH customer_cur into customer_rec; 
      EXIT WHEN customer_cur%notfound; 
      DBMS_OUTPUT.put_line(customer_rec.id || ' ' || customer_rec.name); 
   END LOOP; 
END; 
/
When the above code is executed at the SQL prompt, it produces the following result −

1 Ramesh 
2 Khilan 
3 kaushik 
4 Chaitali 
5 Hardik 
6 Komal  

PL/SQL procedure successfully completed. 



PL/SQL CURSOR CODE
==================

create table ERROR_LOG
( error_code varchar2(20),  error_desc varchar2(500));
  
DECLARE
  CURSOR C IS
    SELECT ROWNUM, EMPNO, ENAME FROM EMP; 
  V1 NUMBER(5);
  V_SQLCODE VARCHAR2(20);
  V_SQLERRM VARCHAR2(400);
BEGIN
  FOR rec IN C LOOP
    BEGIN
      IF rec.ROWNUM = 5 THEN
        DBMS_OUTPUT.PUT_LINE('Error While Processing Record Number: ' || rec.ROWNUM);
        DBMS_OUTPUT.PUT_LINE('Error Description is Captured in Table ERROR_LOG');
        /* Generate an Error @ Line 5 */
        V1 := 'x'; -- Use := for assignment, not =
      ELSE
        DBMS_OUTPUT.PUT_LINE('Processing Record Number: ' || rec.ROWNUM);        
      END IF;
    EXCEPTION
      WHEN OTHERS THEN
        V_SQLCODE := SQLCODE;
        V_SQLERRM := SQLERRM;
        INSERT INTO ERROR_LOG VALUES (V_SQLCODE, V_SQLERRM);
         DBMS_OUTPUT.PUT_LINE('Processing Record Number: ' || V_SQLCODE);
         DBMS_OUTPUT.PUT_LINE('Processing Record Number: ' || V_SQLERRM);
    END;
    V1 := 'x'; 
  END LOOP;
  COMMIT; -- Commit outside the loop
EXCEPTION
  WHEN OTHERS THEN
    V_SQLCODE := SQLCODE;
    V_SQLERRM := SQLERRM;
    ROLLBACK; -- Rollback outside the loop
    INSERT INTO ERROR_LOG VALUES (V_SQLCODE, V_SQLERRM);
         DBMS_OUTPUT.PUT_LINE('Processing Record Number: ' || V_SQLCODE);
         DBMS_OUTPUT.PUT_LINE('Processing Record Number: ' || V_SQLERRM);
    COMMIT; -- Commit after the rollback
END;
/
