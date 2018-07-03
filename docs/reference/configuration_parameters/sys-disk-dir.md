# sys-disk-dir

## Description

Specifies the base path of the default disk store. This directory also holds the data dictionary subdirectory, which stores the persistent data dictionary.

Other SnappyData features also use this directory for storing files. For example, gateway queue overflow and overflow tables use this attribute by default. You can override `sys-disk-dir` for table overflow using options in a table's `CREATE TABLE` statement.

## Usage 

```pre
-spark.snappydata.store.sys-disk-dir=
```

## Default Value

The SnappyData working directory.

## Property Type

connection (boot)

## Prefix

snappydata.
