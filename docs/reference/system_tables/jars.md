# JARS

A SnappyData virtual table that describes installed JAR files.

See [Storing and Loading JAR Files in SnappyData](../../../concepts/developing_applications/storing_and_loading_jar_files/).

<a id="reference_21873F7CB0454C4DBFDC7B4EDADB6E1F__table_BDA0C99BADBA4B3899ECDF79F2E18B0F"></a>

| Column Name | Type    | Length | Nullable | Contents                                                                                                         |
|-------------|---------|--------|----------|------------------------------------------------------------------------------------------------------------------|
| SCHEMA      | VARCHAR | 256    | No       | The schema in which the JAR was installed.                                                                       |
| ALIAS       | VARCHAR | 256    | No       | The name used to refer to this JAR file in commands such as `snappy install-jar`, `remove-jar`, and `replace-jar`. |
| ID          | BIGINT  | 10     | No       | The internal ID of the JAR file installation.                                                                    |

: <span class="tablecap">Table 1. JARS system table</span>


