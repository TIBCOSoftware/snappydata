# snappydata.stream.error.logSeverityLevel

## Description

Logging for messages of different severity levels. Possible values for this property are:

* 0—NO_APPLICABLE_SEVERITY occurs only when the system was unable to determine the severity.

* 10000—WARNING_SEVERITY includes SQLWarnings.

* 20000—STATEMENT_SEVERITY includes errors that cause only the current statement to be aborted.

* 30000—TRANSACTION_SEVERITY includes errors that cause the current transaction to be aborted.

* 40000—SESSION_SEVERITY includes errors that cause the current connection to be closed.

* 45000—DATABASE_SEVERITY includes errors that cause the current database to be closed.

* 50000—SYSTEM_SEVERITY includes internal errors that cause the system to shut down.

## Default Value

0

## Property Type

connection (boot)

## Prefix

n/a
