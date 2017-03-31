# SYSSTATISTICS

Describes statistics collected in the distributed system.

<a id="rrefsistabslilanguageitemsysstatistics__section_91A222DAF49D4832AFD1D777805268B2"></a><a id="rrefsistabslilanguageitemsysstatistics__table_9A65E00BDC1C499194A123AEBB69DB2A"></a>

|Column Name|Type|Length|Nullable|Contents|
|-|-|-|-|-|
| STATID|CHAR|36|No|Unique identifier for the statistic|
| REFERENCEID|CHAR|36|No|The conglomerate for which the statistic was created (join with SYSCONGLOMERATES. CONGLOMERATEID)|
| TABLEID|CHAR|36|No|The table for which the information is collected|
| CREATIONTIMESTAMP|TIMESTAMP|26|No|Time when this statistic was created or updated|
| TYPE|CHAR|1|No|Type of statistics|
| VALID|BOOLEAN|1|No|Whether the statistic is still valid|
| COLCOUNT|INTEGER|10|No|Number of columns in the statistic|
| STATISTICS|`com.pivotal. snappydata.internal. catalog.Statistics`</br>This class is not part of the public API.|2,147,483,647|No|Statistics information|	