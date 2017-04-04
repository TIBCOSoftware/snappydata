---
title: dataextractor
---

Operates against a set of available RowStore operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system.

Syntax
------

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

See also <a href="../../disk_storage/extractor_overview.html#disk_storage" class="xref" title="In cases where disk store files become corrupted, or where you cannot restore disk store backups to members, RowStore provides a data extractor utility that attempts to recover as much data as possible from available disk store files. The recovered data is stored in multiple comma-separated values (CSV) files, which you can use to load the data into a new RowStore system.">Recovering Data from Disk Stores</a> for limitations, requirements, and examples of using this utility.

<table>
<colgroup>
<col width="50%" />
<col width="50%" />
</colgroup>
<thead>
<tr class="header">
<th>Option</th>
<th>Description</th>
</tr>
</thead>
<tbody>
<tr class="odd">
<td>property-file=&lt;filename&gt;</td>
<td>(Required.) The properties file defines the full list of disk store files that are available for each host in the RowStore distributed system you are attempting to recover. Each line in the properties file describes the disk store directories for a single member of the RowStore distributed system, and uses the format:
<pre class="pre codeblock"><code>hostname=path-to-server-directory,path-to-disk-store[,path-to-diskstore]...`</pre>
<p class="note"><strong>Note:</strong> The first value that follows the hostname must specify the full path to the member's working directory. Additional paths can be added to specify the locations of additional disk store files as needed.</p> <p class="note"><strong>Note:</strong> The disk store files that contain the persistent data dictionary are required in order to use the `dataextractor` utility. Each locator and data store member of the distributed system persists the data dictionary in the <span class="ph filepath">/datadictionary</span> subdirectory of the member working directory.</p></td>
</tr>
<tr class="even">
<td>--use-single-ddl=&lt;filename&gt;</td>
<td>By default the utility constructs DDL files using the data dictionaries provided in the properties file list. You can optionally specify a single DDL file to use for recovering data. If you specify a DDL file, then the utility only recovers data for the schema defined in that file, rather than all schemas discovered in the disk store files.</td>
</tr>
<tr class="odd">
<td>--string-delimiter=&quot;&lt;delimiter&gt;&quot;</td>
<td>Specifies a custom delimiter to use in generated CSV files. The default is a double quotation mark: &quot;</td>
</tr>
<tr class="even">
<td>--help</td>
<td>Displays usage information.</td>
</tr>
<tr class="odd">
<td>--output-dir=&lt;path&gt;</td>
<td>Specifies a custom location to store generated output files. By default, all output is placed in a subdirectory named <span class="ph filepath">EXTRACTED_FILES</span> in the current working directory.</td>
</tr>
<tr class="even">
<td>--save-in-server-working-dir=&lt;true | false&gt;</td>
<td>Specifies whether the utility should store recovered data files in the working directory of each server. The default is &quot;false.&quot;</td>
</tr>
<tr class="odd">
<td>--log-file=&lt;filename&gt;</td>
<td>Specifies a custom file in which to store the `dataextractor` output messages. By default this information is stored in <span class="ph filepath">extractor.log</span> in the output directories.</td>
</tr>
<tr class="even">
<td>--num-threads=&lt;threads&gt;</td>
<td><p>The `dataextractor` utility attempts to calculate the size of the target disk stores, and spawns multiple threads in order to extract data as fast as possible. The number of threads is determined by how much heap memory you provide to the utility. Use this option as necessary to override the number of threads that the utility spawns. See <a href="../../disk_storage/extractor_topics.html#topic_cqs_k5c_24" class="xref" title="This section describes some common errors that can occur while recovering data or loading recovered data into a new system.">Troubleshooting Data Recovery Errors</a>.</p></td>
</tr>
<tr class="odd">
<td>--user-name=&lt;user&gt;</td>
<td>Sets the user-name to use for extracting table data. This option can be used in situations where tables were created by an user, in which case the schema name for those tables corresponds to the user name.</td>
</tr>
</tbody>
</table>

<a id="reference_13F8B5AFCD9049E380715D2EF0E33BDC__section_050663B03C0A4C42B07B4C5F69EAC95D"></a>
Description
-----------

Whenever possible, Pivotal recommends that you recover data using an online or offline disk store backup, using the techniques described in <a href="../../disk_storage/backup_restore_disk_store.html#backup_restore_disk_store" class="xref" title="When you invoke the gfxd backup command, RowStore backs up disk stores for all members that are running in the distributed system at that time. Each member with persistent data creates a backup of its own configuration and disk stores.">Backing Up and Restoring Disk Stores</a>. The data recovery utilities provide a "best effort" attempt to recover data in cases where disk store files are corrupted and no viable backup is available.

See <a href="../../disk_storage/extractor_overview.html#disk_storage" class="xref" title="In cases where disk store files become corrupted, or where you cannot restore disk store backups to members, RowStore provides a data extractor utility that attempts to recover as much data as possible from available disk store files. The recovered data is stored in multiple comma-separated values (CSV) files, which you can use to load the data into a new RowStore system.">Recovering Data from Disk Stores</a> for more information.

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

See also <a href="../../disk_storage/extractor_topics.html#topic_ddt_gbx_d4" class="xref" title="Follow these steps to extract available data from available RowStore disk store files.">Procedure for Recovering Data from Disk Stores</a>.


