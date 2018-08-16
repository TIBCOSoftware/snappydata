# Implementing  Row Level Security

The following topics are covered in this section:

*	[Overview of Row Level Security](#rlsoverview)
*	[Creating a Policy](#createpolicy)
*	[Enabling Row Level Security](#enablerowlevelsecurity)
*	[Viewing Policy Details](#viewpolicy)
*	[Combining Multiple Policies](#combinemulpolicies)
*	[Creating Join Queries Using a Policy](#createjoinqueries)
*	[Dropping a Policy](#droppingpolicy)

<a id= rlsoverview> </a>
## Overview of Row Level Security
Policy is a rule that is implemented by using a filter expression.  In SnappyData, you can apply security policies to a table at row level that can restrict, on a per-user basis, the rows that must be returned by normal queries or inserted, updated, or deleted by data modification commands.
To restrict the permissions of a user at row level, you must [create a simple policy](#createpolicy) for a table that can be accessed by several users and then [enable the row level security](#enablerowlevelsecurity) for that table. 

<a id= createpolicy> </a>
## Creating a Policy
A policy, which is a rule, can be created for row tables, column tables, users as well as for the queries that are executed on a table. Users must be granted permissions on DML ops to access the tables for which policies are created.
Here in the following example, a table named **clients** is [created](https://snappydatainc.github.io/snappydata/reference/sql_reference/create-table/#create-table) and access permissions are [granted](https://snappydatainc.github.io/snappydata/reference/sql_reference/grant/) to three users:

```
# Create table named clients.
CREATE TABLE clients (
    ID INT PRIMARY KEY,
    ACCOUNT_NAME STRING NOT NULL,
    ACCOUNT_MANAGER STRING NOT NULL
) USING ROW OPTIONS();

# Grant access to three users.
GRANT SELECT ON clients TO tom, harris, greg;
```

Initially all the users can view all the records in the table. You can restrict the permissions for viewing the records, only after you create a policy and enable the [row level security](#enablerowlevelsecurity). For example, you are  connected to database **testrls** as user **tom**. Initially user **tom** can view all the records from the table as shown:

```
$ SELECT * FROM clients;
 id | account_name | account_manager
----+--------------+-----------------
  1 | jnj          | tom
  2 | tmg          | harris
  3 | tibco        | greg
(3 rows)
```
You can create a policy for a table which can be applied to an AD group, currently logged-in user, or a session user using the following syntax. 

```
CREATE POLICY name ON table_name
	 FOR SELECT
     [ TO { AD GROUP | CURRENT_USER | SESSION_USER } [, ...] ]
    [ USING ( using_expression ) ]
```

!!!Note
	Policies are restrictive by default.
    
In the following example, we create a policy named **just_own_clients** for the user  **tom**.

```
CREATE POLICY just_own_clients ON clients
    FOR SELECT
    TO tom
    USING ( ACCOUNT_MANAGER = 'tom' );
```

After the row level security is enabled, the policy gets applied to the users.

<a id= enablerowlevelsecurity> </a>
## Enabling Row Level Security
The policy which is created to restrict row level permissions is effective only after you enable the row level security. To enable row level security, execute the following **ALTER DDL** command:

```
ALTER TABLE <table_name> ENABLE ROW LEVEL SECURITY;
```
For example, 

```
ALTER TABLE clients ENABLE ROW LEVEL SECURITY;
```

Now the users are permitted to view the records of only those rows that are permitted on the basis of the policy. For example, user **tom** is allowed to view the records of a specific row in table clients.

```
// tom is the user
$ SELECT * FROM clients;
 id | account_name | account_manager 
----+--------------+-----------------
  2 | tmg          | tom
(1 row)

```

<a id= viewpolicy> </a>
## Viewing Policy Details
The policy details can be viewed from a virtual table **SYS.SYSPOLICIES**. **From where can you access this file??** 
The details of the policy are shown in the following columns:

| **Column** | **Description** |
|--------|--------|
|  **NAME**      |  Name of the policy.      |
|    **Using**    |     Filter expression as string.   |
|    **TABLESCHEMANAME**    |     Schema Name of the table on which policy is applied.   |
|    **TABLE_ID**    |  Table on which policy is applied. The internal ID of the table foreign Key constraint which is mapped to **SysTables.ID **   |
|    **TABLENAME**    |  Table on which policy is applied.      |
|   **POLICYFOR**     | The operation for which the policy is intended. For example, SELECT, UPDATE, INSERT etc. For now it will be only “SELECT”  |
|    **APPLYTO**    |  The comma separated string of User Names ( or current_user) or LDAP group on which the policy applies.      |
|  **FILTER**      |  The filter associated with the policy.      |
|   **OWNER**     |   Owner of the policy which is the same as the owner of the target table.|
|   **ENABLED**     |  Indication whether the policy is enabled or not. |
|   **USER**     |  The user on which the policy is applied. This can be a session user or the current user.|
|   **AD GROUP**     |  The list of individual users in a AD group on which the policy is applied.|
|   **POLICY_ID**     |  The foreign key on **SYSPOLICY.ID**|

<a id= combinemulpolicies> </a>
## Combining Multiple Policies
Multiple policies can be defined for a table and combined as a statement. As policies are table-specific, each policy, that is applied to a table, must have a unique name.  Tables in different schemas can have policies with the same name which can then have different conditions. If multiple policies are applied on a table, the filters will be evaluated as **AND**.

Here in the following example, multiple policies are created for the table named **mytable** and row level security is enabled. The current user here is **tom**.

```
CREATE POLICY mypolicy1 on mytable using (user_col = current_user);
CREATE POLICY mypolicy2 on mytable using (id < 4);
CREATE POLICY mypolicy3 on mytable using (account_name = ‘tibco’);

ALTER TABLE mytable ENABLE ROW LEVEL SECURITY;

```
These policies are combined as shown in this example:

```
SELECT * FROM mytable
WHERE (user_col = current_user) # current_user is  <table owner>
AND ((id<4)
AND(account_name = ‘tibco’);

$ select * from mytable;
 id | account_name | account_manager 
----+--------------+-----------------
  3 | tibco        | tom

(1 row)

```

<a id= createjoinqueries> </a>
## Creating Join Queries Using a Policy

Refer the following sample, to create join queries using a policy. 

```
CREATE TABLE customers (
    customer_id int,
    name string,
    hidden boolean
) using column options();

INSERT INTO customers (customer_id, name)...

CREATE TABLE orders (
    order_id int,
    customer_id int
) using column options();

INSERT INTO orders (order_id, customer_id) ...

An untrusted user that will be doing SELECTs only:

Let the untrusted user group be named by Ldap (AD group) group untrusted_group

GRANT SELECT ON customers TO ldapgroup:untrusted_group;
GRANT SELECT ON orders TO ldapgroup:untrusted_group;

A policy that makes hidden customers invisible to the untrusted user:

CREATE POLICY no_hidden_customers ON customers FOR SELECT TO ldapgroup:untrusted_group USING (hidden is false);
ALTER TABLE customers ENABLE ROW LEVEL SECURITY;
SELECT name FROM orders JOIN customers ON  customer_id WHERE order_id = 4711;
```

<a id= droppingpolicy> </a>
## Dropping a Policy
Since the policy is dependent on the table, only the owner of the table can drop a policy.

To drop a policy, use the following syntax:

```
DROP POLICY policy_name
```
For example,

```
DROP POLICY just_own_clients
```
!!!Caution
	You cannot delete a table, if there is a policy associated with it.  You must drop the policy for a table and only then delete the table.