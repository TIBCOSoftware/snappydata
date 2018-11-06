# Implementing  Row Level Security

<ent>This feature is available only in the Enterprise version of SnappyData. </br></ent>

The following topics are covered in this section:

*	[Overview of Row Level Security](#rlsoverview)
*	[Activating Row Level Security](#actrowlevel)
*	[Creating a Policy](#createpolicy)
*	[Enabling Row Level Security](#enablerowlevelsecurity)
*	[Viewing Policy Details](#viewpolicy)
*	[Combining Multiple Policies](#combinemulpolicies)
*	[Policy Application on Join Queries](#createjoinqueries)
*	[Dropping a Policy](#droppingpolicy)

<a id= rlsoverview> </a>
## Overview of Row Level Security
Policy is a rule that is implemented by using a filter expression.  In SnappyData, you can apply security policies to a table at row level that can restrict, on a per-user basis, the rows that must be returned for normal queries by data modification commands. 
For [activating this row level security](#actrowlevel), a system property must be added to the configuration files of servers, leads, and locators. 
To restrict the permissions of a user at row level, [create a simple policy](#createpolicy) for a table that can be applied on a per user basis and then [enable the row level security](#enablerowlevelsecurity) for that table.

<a id= actrowlevel> </a>
## Activating Row Level Security
For activating Row Level Security, a system property `-J-Dsnappydata.enable-rls=true` must be added to the configuration files of servers, leads, and locators when you [configure the cluster](/configuring_cluster/configuring_cluster.md). By default this is off.
If this property is not added, you cannot enable the Row Level Security and an exception is thrown when you attempt to create the policy.

!!! Warning
	When this property is set to **true**, the Smart Connector access to SnappyData will fail with `java.lang.IllegalStateException: Row level security (snappydata.enable-rls) does not allow smart connector mode` exception.

<a id= createpolicy> </a>
## Creating a Policy
A policy, which is a rule, can be created for row tables, column tables, and external GemFire tables.  Permissions can be granted to users on DML ops to access the tables for which policies are created.

!!!Note
	Policies are restrictive by default.

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

Initially all the users can view all the records in the table. You can restrict the permissions for viewing the records,  only after you enable the [row level security](#enablerowlevelsecurity) and apply a policy. For example, you are  connected to database **testrls** as user **tom**. Initially user **tom** can view all the records from the table as shown:

```
$ SELECT * FROM clients;
 id | account_name | account_manager
----+--------------+-----------------
  1 | jnj          | tom
  2 | tmg          | harris
  3 | tibco        | greg
(3 rows)
```

You can create a policy for a table which can be applied to a user or a LDAP group using the following syntax. 

```
CREATE POLICY name ON table_name
	 FOR SELECT
     [ TO { LDAP GROUP | CURRENT_USER | <USER_NAME> } [, ...] ]
    [ USING ( using_expression ) ]
```
!!!Note
	CURRENT_USER implies to any user who is excecuting the query. 
    
    
In the following example, we create a policy named **just_own_clients** where a user can view only the row where that user is the account manager.
For example, if we want this rule to apply only to user **Tom**, then we can create the policy as shown:

```
CREATE POLICY just_own_clients ON clients
    FOR SELECT
    TO TOM
    USING ACCOUNT_MANAGER = CURRENT_USER();
```
As per the the above policy, user **Tom** can see only one row, where as other users can view all the rows.

The same can be also applied to a LDAP group as shown:

```
CREATE POLICY just_own_clients ON clients
    FOR SELECT
    TO ldapgroup:group1
    USING ACCOUNT_MANAGER = CURRENT_USER();
```

After the row level security policy is enabled, the policy gets applied to the corresponding users.

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

Now the users are permitted to view the records of only those rows that are permitted on the basis of the policy. For example, user **tom** is allowed to view the records of a specific row in table **clients**.

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

The policy details can be viewed from a virtual table named **SYS.SYSPOLICIES**. The details of the policy are shown in the following columns:

| **Column** | **Description** |
|--------|--------|
|  **NAME**      |  Name of the policy.      |
|    **SCHEMANAME**    |     Schema Name of the table on which policy is applied.   |
|    **TABLENAME**    |  Table on which policy is applied.      |
|   **POLICYFOR**     | The operation for which the policy is intended. For example, SELECT, UPDATE, INSERT etc. For now it will be only “SELECT”  |
|    **APPLYTO**    |  The comma separated string of User Names ( or CURRENT_USER) or LDAP group on which the policy applies.      |
|  **FILTER**      |  The filter associated with the policy.      |
|   **OWNER**     |   Owner of the policy which is the same as the owner of the target table.|


<a id= combinemulpolicies> </a>
## Combining Multiple Policies
Multiple policies can be defined for a table and combined as a statement. As policies are table-specific, each policy, that is applied to a table, must have a unique name.  Tables in different schemas can have policies with the same name which can then have different conditions. If multiple policies are applied on a table, the filters will be evaluated as **AND**.

Here in the following example, multiple policies are created for the table named **mytable** and row level security is enabled. The current user here is **tom**.

```
CREATE POLICY mypolicy1 on mytable using user_col = current_user();
CREATE POLICY mypolicy2 on mytable using id < 4;
CREATE POLICY mypolicy3 on mytable using account_name = ‘tibco’;

ALTER TABLE mytable ENABLE ROW LEVEL SECURITY;

```
These policies are combined as shown in this example:

```
SELECT * FROM mytable
WHERE user_col = current_user() # current_user is  <table owner>
AND id<4
AND account_name = ‘tibco’;

$ select * from mytable;
 id | account_name | account_manager 
----+--------------+-----------------
  3 | tibco        | tom

(1 row)

```

<a id= createjoinqueries> </a>
## Policy Application on Join Queries

Refer the following example for policy application in case of join queries: 

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

Let the untrusted user group be named by LDAP (AD group) group as untrusted_group

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
	If you drop a table, all the policies associated with the table will also get dropped.