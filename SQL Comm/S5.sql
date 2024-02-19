SELECT SUBSTR('Deepak.Kr.Baran@gmail.com', INSTR('Deepak.Kr.Baran@gmail.com', '@')) AS domain_part
FROM dual;

OdiStartLoadPlan "-LOAD_PLAN_NAME=Vij_LP" "-CONTEXT=DEVSCOTT" "-AGENT_CODE=Vijay_ODIAgent_LA" "-AGENT_URL=http://localhost:20910/oraclediagent" 
"-ODI_USER=SUPERVISOR" "-ODI_PASS=gHypGAMF2En9zq,wvgEl" "-LOG_LEVEL=6"

Low-Cardinality:
In low-cardinality columns, there are relatively few unique values compared to the total number of rows in the table.
These columns often contain repeated or duplicated values.
Example: A column like "Gender" with only two possible values, "Male" and "Female", in a table with millions of rows would have low cardinality.
Low-cardinality columns are typically not useful for indexing because they dont provide much selectivity. 
Indexing such columns might not improve query performance significantly.

High-Cardinality:
High-cardinality columns have a large number of unique values compared to the total number of rows.
Each value in a high-cardinality column tends to be unique or nearly unique.
Example: A column like "CustomerID" in a table with millions of rows where each row has a distinct customer ID would have high cardinality.
High-cardinality columns are often good candidates for indexing because they provide better selectivity. 
Indexing such columns can significantly improve query performance, especially for queries that filter or join on these columns.
In summary, the main difference lies in the number of unique values present in the column. 
Low-cardinality columns have relatively few unique values, while high-cardinality columns have a large number of unique values. 
This difference impacts database design, indexing strategies, and query optimization techniques.

INDEXES
=======
Primary Key Constraint: When you define a primary key on a table, Oracle automatically creates a unique index on the primary key column(s). 
This index enforces the uniqueness of values in the primary key column(s) and provides fast access to rows based on the primary key.

Unique Constraint: Similarly, when you define a unique constraint on a column or set of columns, 
Oracle automatically creates a unique index to enforce the uniqueness constraint.

Bitmap Indexes for Materialized Views: If you create a materialized view with a fast refresh option, 
Oracle might automatically create bitmap indexes on the materialized views columns to facilitate fast refresh operations.

Other than these automatic index creations, all other indexes in Oracle must be explicitly created by users or database 
administrators based on the specific requirements of the database schema and the queries executed against it.

For example, if a user issues a SQL statement like:

CREATE INDEX my_index ON my_table (my_column);
And does not specify the index type, Oracle will create a B-tree index named my_index on the column my_column in the table my_table


===========

Storage Size:

VARCHAR: In Oracle versions prior to 12c, VARCHAR was used to define variable-length character strings of up to 2000 bytes. 
However, its deprecated now and not commonly used.
VARCHAR2: VARCHAR2 is the preferred data type for variable-length character strings. It can store variable-length character data of up to 4000 bytes in size.
Handling of NULL Values:

VARCHAR: In older versions of Oracle, VARCHAR doesnt distinguish between an empty string and a NULL value. This can lead to some ambiguities.
VARCHAR2: VARCHAR2 properly distinguishes between an empty string and NULL values. 
This means you can assign an empty string to a VARCHAR2 column without it being considered NULL.
Performance:

VARCHAR2 is generally more efficient in terms of performance compared to VARCHAR. 
This is because VARCHAR2 doesnt require extra space to store information about the length of the string, as its automatically managed by Oracle.


Git Hub in ODI 12c
==================
 
Go to ODI Studio--->Goto Tools-->Switch/Versioning Applications
ODI Supports SubVersion(From Apache)
Git (Use Git Repository for Versioning ODI objects)
