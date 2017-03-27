# INDEXES

A SnappyData virtual table that describes table indexes.

See <a href="../language_ref/ref-create-index.html#create-index" class="xref" title="Creates an index on one or more columns of a table.">CREATE INDEX</a>.

<a id="reference_21873F7CB0454C4DBFDC7B4EDADB6E1F__table_9DCAD37327BD4CCBAF98E8689F175144"></a>

| Column Name         | Type    | Length | Nullable | Contents                                                                                     |
|---------------------|---------|--------|----------|----------------------------------------------------------------------------------------------|
| SCHEMANAME          | VARCHAR | 128    | No       | The schema in which the index resides.                                                       |
| TABLENAME           | VARCHAR | 128    | No       | The table on which the index was created.                                                    |
| INDEXNAME           | VARCHAR | 128    | No       | The index name that was provided in the `CREATE INDEX` command.                              |
| COLUMNS\_AND\_ORDER | VARCHAR | 256    | No       | The table columns on which the index was created.                                            |
| UNIQUE              | VARCHAR | 64     | No       | Whether the index is UNIQUE or NOT\_UNIQUE.                                                  |
| CASESENSITIVE       | BOOLEAN | 1      | No       | Whether the index is case-sensitive (the default) or case-insensitive.                       |
| INDEXTYPE           | VARCHAR | 128    | No       | A description of the index is local or a global hash index, and whether the index is sorted. |

<!--: <span class="tablecap">Table 1. INDEXES system table</span>
-->

