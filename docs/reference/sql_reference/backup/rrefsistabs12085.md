# SYSSCHEMAS

Describes all schemas in the distributed system.

<a id="rrefsistabs12085__section_5A60E772D103431AA37F96928433A242"></a><a id="rrefsistabs12085__table_EE876FA26B9B486F9AB2A07CE824762D"></a>

| Column Name         | Type    | Length | Nullable | Contents                                                    |
|---------------------|---------|--------|----------|-------------------------------------------------------------|
| SCHEMAID            | CHAR    | 36     | No       | Unique identifier for the schema                            |
| SCHEMANAME          | VARCHAR | 128    | No       | Schema name                                                 |
| AUTHORIZATIONID     | VARCHAR | 128    | No       | The authorization identifier of the owner of the schema     |
| DEFAULTSERVERGROUPS | VARCHAR | 128    | No       | The default server group(s) to use for tables in the schema |



