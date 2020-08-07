# skip-locks

## Description

!!! Note 
	This property is provided only for the purpose of cancelling a long-running query in cases where the query causes a DDL operation to hold a DataDictionary lock, preventing new logins to the system. Using this property outside of its intended purpose can lead to data corruption, especially if DDL is performed while the property is enabled. </p>

`skip-locks` forces the associated connection to avoid acquiring DataDictionary and table locks, enabling a JVM owner user to log into a system where a blocked DDL operation holds a DataDictionary lock and prevents new connections. Any operation that attempts to acquire a table or DataDictionary lock from the connection logs a warning and sends a SQLWarning in the statement. Transaction locks are still obtained as usual.

Use this property to connect directly to a TIBCO ComputeDB server, rather than a locator. (The property disables the load-balance property by default, as load balancing can cause local deadlocks even when `skip-locks` is enabled.) 

This property is restricted to JVM owners. Attempting to set the property without JVM owner credentials fails with the error, "Connection refused: administrator access required for skip-locks." If authorization is disabled, the default user "APP" is the JVM owner.

## Default Value

false

## Property Type

connection

## Prefix

n/a
