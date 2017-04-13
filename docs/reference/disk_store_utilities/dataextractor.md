# dataextractor

Operates against a set of available SnappyData operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system.

## Syntax

``` pre
dataextractor  property-file=<filename>
  [--use-single-ddl=<filename>]
  [--string-delimiter="<delimiter>"]
  [--help]  
  [--output-dir=<path>]
  [--save-in-server-working-dir=<true | false>]
  [--log-file=<filename>]
  [--num-threads=<threads>]
  [--user-name=<user>]
```

See also [Recovering Data from Disk Stores](../../concepts/tables/persisting_table_data/extractor_overview.md#disk_storage) for limitations, requirements, and examples of using this utility.

| Option |Description |
|--------|--------|
|property-file=&lt;filename&gt;|(Required.) The properties file defines the full list of disk store files that are available for each host in the SnappyData distributed system you are attempting to recover. Each line in the properties file describes the disk store directories for a single member of the SnappyData distributed system, and uses the format: </br><pre class="pre codeblock"><code>hostname=path-to-server-directory,path-to-disk-store[,path-to-diskstore]...</code></pre> </br> **Note**: </br>*The first value that follows the hostname must specify the full path to the member's working directory. Additional paths can be added to specify the locations of additional disk store files as needed. </br> * The disk store files that contain the persistent data dictionary are required in order to use the <code class="ph codeph">dataextractor</code> utility. Each locator and data store member of the distributed system persists the data dictionary in the <span class="ph filepath">/datadictionary</span> subdirectory of the member working directory.|
|--use-single-ddl=&lt;filename&gt;|By default the utility constructs DDL files using the data dictionaries provided in the properties file list. You can optionally specify a single DDL file to use for recovering data. If you specify a DDL file, then the utility only recovers data for the schema defined in that file, rather than all schemas discovered in the disk store files.|
|--string-delimiter=&quot;&lt;delimiter&gt;&quot;|Specifies a custom delimiter to use in generated CSV files. The default is a double quotation mark: &quot;|
|--help|Displays usage information.|
|--output-dir=&lt;path&gt;|Specifies a custom location to store generated output files. By default, all output is placed in a subdirectory named <span class="ph filepath">EXTRACTED_FILES</span> in the current working directory.|
|--save-in-server-working-dir=<true false>|Specifies whether the utility should store recovered data files in the working directory of each server. The default is &quot;false.&quot;|
|--log-file=&lt;filename&gt;|Specifies a custom file in which to store the <code class="ph codeph">dataextractor</code> output messages. By default this information is stored in <span class="ph filepath">extractor.log</span> in the output directories.|
|--num-threads=&lt;threads&gt;|<p>The <code class="ph codeph">dataextractor</code> utility attempts to calculate the size of the target disk stores, and spawns multiple threads in order to extract data as fast as possible. The number of threads is determined by how much heap memory you provide to the utility. Use this option as necessary to override the number of threads that the utility spawns. See [Troubleshooting Data Recovery Errors](../../concepts/tables/persisting_table_data/extractor_topics.md#troubleshooting-data-recovery-errors).|
|--user-name=&lt;user&gt;|Sets the user-name to use for extracting table data. This option can be used in situations where tables were created by an user, in which case the schema name for those tables corresponds to the user name.|

<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_050663B03C0A4C42B07B4C5F69EAC95D"></a>
## Description

Whenever possible, SnappyData recommends that you recover data using an online or offline disk store backup, using the techniques described in [Backing Up and Restoring Disk Stores](../../concepts/backup/backup_restore_disk_store.md). The data recovery utilities provide a "best effort" attempt to recover data in cases where disk store files are corrupted and no viable backup is available.

See [Recovering Data from Disk Stores](../../concepts/tables/persisting_table_data/extractor_overview.md#disk_storage)for more information.

Execute the data extractor utility against disk store files specified in a custom <span class="ph filepath">extractor.properties</span> file:

``` pre
$ dataextractor property-file=./extractor.properties 
Reading the properties file : ./extractor.properties
Total size of data to be extracted : 14.4404296875MB
Disk space available in the output directory : 30423.44921875MB
Sufficient disk space to carry out data extraction
Extracting DDL for server : exampleserver1
Extracting DDL for server : examplelocator1
Extracting DDL for server : exampleserver2
Completed extraction of DDL's for server : examplelocator1
Completed extraction of DDL's for server : exampleserver1
Completed extraction of DDL's for server : exampleserver2
NULL ROW FORMATTER FOR:SYSIBMSYSDUMMY1
Maximum disk-store size on disk 5.057651519775391 MB
Available memory : 52.09442901611328 MB
Estimated memory needed per server : 11.12683334350586 MB
Recommended number of threads to extract server(s) in parallel : 4
Started data extraction for Server : examplelocator1
Started data extraction for Server : exampleserver1
Started data extraction for Server : exampleserver2
Extracting disk stores
Extracting disk stores
Server : examplelocator1 Attempting extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/locator
Extracting disk stores
Server : exampleserver2 Attempting extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server2
Server : exampleserver1 Attempting extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server1
Completed extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/locator
Completed extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server2
Completed extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server1
Total Salvage Time : 15.851s
Writing out Summary and Recommendation...
Completed Summary and Recommendation
```

[Procedure for Recovering Data from Disk Stores](../../concepts/tables/persisting_table_data/extractor_topics.md). 


