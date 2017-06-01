# Drop Function

## Syntax
```
DROP FUNCTION IF EXISTS udf_name
```

## Description
Drop an existing function. If the function to drop does not exist, an exception is reported.

## Example
```
DROP FUNCTION IF EXISTS app.strnglen
```