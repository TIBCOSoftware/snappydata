# INDEXES

A SnappyData virtual table that describes table indexes.

See [CREATE INDEX](../../reference/sql_reference/create-index.md).

| Column Name         | Type    | Length | Nullable | Contents                                                                                     |
|---------------------|---------|--------|----------|----------------------------------------------------------------------------------------------|
| SCHEMANAME          | VARCHAR | 128    | No       | The schema in which the index resides.                                                       |
| TABLENAME           | VARCHAR | 128    | No       | The table on which the index was created.                                                    |
| INDEXNAME           | VARCHAR | 128    | No       | The index name that was provided in the `CREATE INDEX` command.                              |
| COLUMNS_AND_ORDER | VARCHAR | 256    | No       | The table columns on which the index was created.                                            |
| UNIQUE              | VARCHAR | 64     | No       | Whether the index is UNIQUE or NOT_UNIQUE.                                                  |
| CASESENSITIVE       | BOOLEAN | 1      | No       | Whether the index is case-sensitive (the default) or case-insensitive.                       |
| INDEXTYPE           | VARCHAR | 128    | No       | A description of the index is local or a global hash index, and whether the index is sorted. |
