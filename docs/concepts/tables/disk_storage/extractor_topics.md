# How the Data Recovery Utilities Work

The `dataextractor` utility operates against a set of available SnappyData operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system. A `dataextractloader` utility takes the CSV file output along with a recommendations file, and uses those inputs to load the recovered data into a new SnappyData system.

In order to recover data from SnappyData disk stores, you begin by making a copy of all available disk files. You then create a properties file to define the full list of disk store files that are available for each host in the SnappyData distributed system you are attempting to recover. Each line in the properties file describes the disk store directories for a single member of the SnappyData distributed system, and uses the format:

``` 
hostname=path-to-server-directory,path-to-disk-store[,path-to-diskstore]...
```

!!!Note
	The first value that follows the hostname must specify the full path to the member's working directory. Additional paths can be added to specify the locations of additional disk store files as needed. </br>Running the [dataextractor](../../../reference/disk_store_utilities/dataextractor.md) utility with your properties file instructs the utility to examine the available disk store files and determine which files contain the most recent version of the persistent data. The utility uses this information to extract as much data as possible, outputting information into the following files:

<a id="disk_storage__table_zhq_gvw_d4"></a>

| File Type	 | Example	 |Description |
|--------|--------|--------|
|     Extract log   | extract.log       |   Full log output for the data extraction process.     |
|     File summary	   |  Summary.txt      |  Specifies a complete list of all SQL and CSV file names that were created during the extraction process.      |
|    Recovery recommendations    |  Recommended.txt      |  Specifies the absolute path of all SQL and CSV files that the utility recommends using for loading into a SnappyData system, in the order that they should be loaded. This represents the "best guess" selection of content that will recover the most data from the available files.      |
|     DDL files.   |  exported_ddl.sql      |    DDL files that can be replayed to create the recovered database objects. </br> All DDL files are created in a subdirectory for each server being recovered.    |
|     Recovered data files   | PR-APP-FLIGHTS-_B__APP_FLIGHTS_91-1400537866176.csv </br>RR-APP-AIRLINES-1400537864113.csv       |  These files contain the recovered data that you can use with [dataextractloader](../../../reference/disk_store_utilities/dataextractor.md) to restore the data in a new distributed system. CSV files are created per table. Partitioned table filenames begin with PR, and replicated table files begin with RR. A partitioned table generates one CSV file per bucket, and the bucket number is present in the filename. All CSV filenames include a timestamp. </br> All CSV files are created in a subdirectory for each server being recovered.      |

After running `dataextractor`, you can choose to use the recommendations file as-is, or edit the file to recover only a portion of the available data. You then run `dataextractloader` with the recommendations file to load the data into a new SnappyData system.

<a id="id_vnc_ysw_d4"></a>

## Limitations for Data Recovery

The `dataextractor` utility provides only a "best effort" attempt to recover disk store data. Keep these limitations in mind when you use the utilities

-   The `dataextractor` utility uses available data to determine which member held the latest version of persistent data. This determination cannot be accurate in all situations.

-   Data recovered by the utility may not be complete or consistent for the database schema. Specifically:

    -   Recovered data may be incomplete (missing data or operations). This may be unavoidable if the disk store that holds the latest version of some data is corrupt and the data cannot be extracted.

    -   Duplicate entries in recovered data may violate unique key constraints when you load the recovered data to a new system.

    -   Missing data in parent-child tables may violate foreign key constraints when you load the recovered data to a new system.

-   The `dataextractor` utility does not recover any index information.

<a id="topic_eks_mxw_d4"></a>

## Requirements

The following procedures and resources are required in order to use the data recovery utilities.

-   You must shut down all members of the system that you want to recover, before you use the `dataextractor` utility.

-   The machine on which you run the `dataextractor` utility cannot run a SnappyData member (locator, datastore, or accessor) for any other SnappyData distributed system.

-   Make a copy of any disk store file that you want to use for recovering data, and run the `dataextractor` utility against your copies of those files.

	!!! Note
    	The disk store files that contain the persistent data dictionary are required in order to use the `dataextractor` utility. Each locator and data store member of the distributed system persists the data dictionary in the <span class="ph filepath">/datadictionary</span> subdirectory of the member working directory.

-   You must allocate enough heap memory to the `dataextractor` utility, using the `-Xmx` argument. The amount of memory required is equal to the size of the largest SnappyData member that you are recovering. For example, if the system that you are recovering had three datastore members with 30 GB, 25 GB, and 15 GB of memory, then you must allocate 30 GB of memory to the `dataextractor` utility process. Additional heap memory is required if you execute the `dataextractor` utility using multiple threads.

-   The machine on which you run `dataextractor` must have twice the amount of disk space available than the total size of the disk store files specified in the input properties file.

<a id="topic_ddt_gbx_d4"></a>

## Procedure for Recovering Data from Disk Stores

Follow these steps to extract available data from available SnappyData disk store files.

!!! Note
	The example commands and output in this section use the persistent schema and SnappyData cluster members that were created using the SnappyData tutorials. If you want to follow these steps as a data recovery tutorial, first complete the tutorial steps through <mark> Persist Tables to Disk TO BE CONFIRMED </mark>.

1.  Shut down all members of the system that you want to recover. For example:

		$ snappy shut-down-all -locators=localhost[10101]
		Connecting to distributed system: locators=localhost[10101]
		Successfully shut down 2 members
		$ snappy locator stop -dir=$HOME/locator
		The SnappyData Locator has stopped.

2.  Shut down any other SnappyData members that may be running on the local machine (for example, locators or data stores that run as part of another SnappyData distributed system).

3.  Make a copy of all disk store files that you want to use for recovery. At a minimum, this involves copying the member working directory for locators and data stores in your system. For example:

	    $ mkdir ~/recovery-directory
    	$ cp -r ~/locator ~/recovery-directory
    	$ cp -r ~/server1 ~/recovery-directory
    	$ cp -r ~/server2 ~/recovery-directory
    
    !!!Note
    	If you created disk store files outside of a member's working directory, ensure that you make a copy of those disk store files as well.</p>

4.  Move to the directory containing the copied disk store files, and create the <span class="ph filepath">extractor.properties</span> file:

        $ cd ~/recovery-directory
    	$ touch extractor.properties
    
5.  Use a text editor edit the <span class="ph filepath">extractor.properties</span> file. Enter a SnappyData member definition on each line, with the first value corresponding to the pathname of the member's working directory. For example:

        recoveredlocator1=/Users/yozie/recovery-directory/locator
    	recoveredserver1=/Users/yozie/recovery-directory/server1
		
    	recoveredserver2=/Users/yozie/recovery-directory/server2
    
    !!! Note
    	If you created disk store files outside of a member's working directory, ensure that you specify the directory location of those disk store files in a comma-separated list **following** the member working directory, as in:

        recoveredserver1=/Users/yozie/recovery-directory/server1,/Users/yozie/recovery-directory/server1-external-diskstores
        
6.  Set the JAVA\_ARGS environment variable to allocate the required heap space (see <a href="#topic_eks_mxw_d4" class="xref" title="The following procedures and resources are required in order to use the data recovery utilities.">Requirements</a>). For example:

        $ export JAVA_ARGS=-Xmx2G
    
7.  Execute the `dataextractor` utility, specifying the properties file that you created:

        $ dataextractor property-file=./extractor.properties 
    	Reading the properties file : ./extractor.properties
    	Total size of data to be extracted : 14.4404296875MB
    	Disk space available in the output directory : 30423.44921875MB
    	Sufficient disk space to carry out data extraction
    	Extracting DDL for server : recoveredserver1
    	Extracting DDL for server : recoveredlocator1
    	Extracting DDL for server : recoveredserver2
    	Completed extraction of DDL's for server : recoveredlocator1
    	Completed extraction of DDL's for server : recoveredserver1
    	Completed extraction of DDL's for server : recoveredserver2
    	NULL ROW FORMATTER FOR:SYSIBMSYSDUMMY1
    	Maximum disk-store size on disk 5.057651519775391 MB
    	Available memory : 52.09442901611328 MB
    	Estimated memory needed per server : 11.12683334350586 MB
    	Recommended number of threads to extract server(s) in parallel : 4
    	Started data extraction for Server : recoveredlocator1
    	Started data extraction for Server : recoveredserver1
    	Started data extraction for Server : recoveredserver2
    	Extracting disk stores
    	Extracting disk stores
    	Server : recoveredlocator1 Attempting extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/locator
    	Extracting disk stores
    	Server : recoveredserver2 Attempting extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server2
    	Server : recoveredserver1 Attempting extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server1
    	Completed extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/locator
    	Completed extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server2
    	Completed extraction of diskstore:GFXD-DEFAULT-DISKSTORE from directory: /Users/yozie/recovery-directory/server1
    	Total Salvage Time : 15.851s
    	Writing out Summary and Recommendation...
    	Completed Summary and Recommendation
    
	!!!Note:
		See <a href="../reference/disk-store-utilities/dataextractor.html#reference_13F8B5AFCD9049E380715D2EF0E33BDC" class="xref" title="Operates against a set of available SnappyData operational log files (disk stores) in order to extract data to CSV files and provide recommendations for how to best restore the data in a new distributed system.">dataextractor</a> for a full description of additional command-line options.</p>

    Output from the utility is stored in two subdirectories of the working directory, named <span class="ph filepath">EXTRACTED\_FILES</span> and <span class="ph filepath">datadictionary</span>. For example:

        $ ls
    	EXTRACTED_FILES/      datadictionary/       extractor.properties  locator/              server1/              server2/
    
    The <span class="ph filepath">Summary.txt</span> file is stored in the last extracted files directory:

    	
            $ cat EXTRACTED_FILES/Summary.txt 
            [DDL EXPORT INFORMATION]

          1. recoveredlocator1 , file : /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredlocator1/exported_ddl.sql Number of ddl statements : 9
          2. recoveredserver1 , file : /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/exported_ddl.sql Number of ddl statements : 9
          3. recoveredserver2 , file : /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver2/exported_ddl.sql Number of ddl statements : 9


        [EXPORT INFORMATION FOR TABLES]
        Table:APP_FLIGHTS__B__APP_FLIGHTS_36
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_36-1400537865621.csv . Number of rows extracted : 3

        Table:APP_FLIGHTS__B__APP_FLIGHTS_37
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_37-1400537866233.csv . Number of rows extracted : 5

        Table:APP_FLIGHTS__B__APP_FLIGHTS_34
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver2/PR-APP-FLIGHTS-_B__APP_FLIGHTS_34-1400537866329.csv . Number of rows extracted : 5

        Table:APP_FLIGHTS__B__APP_FLIGHTS_35
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_35-1400537865998.csv . Number of rows extracted : 5

        Table:APP_FLIGHTS__B__APP_FLIGHTS_38
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_38-1400537866224.csv . Number of rows extracted : 11

        Table:APP_FLIGHTS__B__APP_FLIGHTS_39
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_39-1400537865896.csv . Number of rows extracted : 5

        Table:APP_FLIGHTAVAILABILITY__B__APP_FLIGHTAVAILABILITY_28
          1. /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver2/PR-APP-FLIGHTAVAILABILITY-_B__APP_FLIGHTAVAILABILITY_28-1400537866855.csv . Number of rows extracted : 14
        [...]
        ```

    The DDL EXPORT INFORMATION shows the order in which the utility recommends replaying DDL files to restore the data dictionary. You can review the DDL files to ensure that the tables match your expected schema. Comments are inserted to call out replicated and partitioned tables, as well as table colocation.

    This is followed by a list of CSV files that contain the data values to load into the tables. In the example above, you can see that FLIGHTS is a partitioned table, and a separate CSV file is generated per bucket of the table. The file summary shows the number of rows recovered for each bucket of the table.

<a id="topic_o3x_vfc_24"></a>

## Procedure for Loading Recovered Data into a New System

Follow these steps to load the SQL and CSV files that were recovered using `dataextractor` into a new SnappyData system.

!!!Note 
	The example commands and output in this section use the example recovery files that were generated in <a href="#topic_ddt_gbx_d4" class="xref" title="Follow these steps to extract available data from available SnappyData disk store files.">Procedure for Recovering Data from Disk Stores</a>.</p>

1.  Boot a new SnappyData distributed system into which you will load the recovered data. Ensure that you define the necessary server groups, heap configuration, and disk resources needed to host the recovered data. Refer to the DDL EXPORT INFORMATION portion of the <span class="ph filepath">Summary.txt</span> file to determine which server groups are expected when recreating the schema.

	If you are continuing with the example cluster recovered in <a href="#topic_ddt_gbx_d4" class="xref" title="Follow these steps to extract available data from available SnappyData disk store files.">Procedure for Recovering Data from Disk Stores</a>, then a single datastore is sufficient to reload the sample data. Create and start the new datastore directly in the recovery subdirectory:

        $ cd ~/recovery-directory
        $ mkdir recovery-server
        $ snappy rowstore server start -dir=./recovery-server/
        Starting SnappyData Server using multicast for peer discovery: 239.192.81.1[10334]
        Starting network server for SnappyData Server at address localhost/127.0.0.1[1527]
        Logs generated in /Users/yozie/recovery-directory/./recovery-server/gfxdserver.log
        SnappyData Server pid: 4674 status: running

2.  Run the `dataextractloader` utility, specifying the <span class="ph filepath">Recommended.txt</span> file that was created during recovery and the hostname and port number of a locator or server to use for connecting to the new SnappyData system. For example:

        $ dataextractloader host=localhost port=1527 recommended=./EXTRACTED_FILES/Recommended.txt 
    	Loading .sql file: /Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredlocator1/exported_ddl.sql
    	Executing :CALL SYSCS_UTIL.IMPORT_TABLE_EX ('APP', 'FLIGHTS', '/Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_36-1400537865621.csv' , ',', '"', null, 0, 0, 6, 0, null, null)
    	Executing :CALL SYSCS_UTIL.IMPORT_TABLE_EX ('APP', 'FLIGHTS', '/Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_37-1400537866233.csv' , ',', '"', null, 0, 0, 6, 0, null, null)
    	Executing :CALL SYSCS_UTIL.IMPORT_TABLE_EX ('APP', 'FLIGHTS', '/Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver2/PR-APP-FLIGHTS-_B__APP_FLIGHTS_34-1400537866329.csv' , ',', '"', null, 0, 0, 6, 0, null, null)
    	Executing :CALL SYSCS_UTIL.IMPORT_TABLE_EX ('APP', 'FLIGHTS', '/Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_35-1400537865998.csv' , ',', '"', null, 0, 0, 6, 0, null, null)
    	Executing :CALL SYSCS_UTIL.IMPORT_TABLE_EX ('APP', 'FLIGHTS', '/Users/yozie/recovery-directory/EXTRACTED_FILES/recoveredserver1/PR-APP-FLIGHTS-_B__APP_FLIGHTS_38-1400537866224.csv' , ',', '"', null, 0, 0, 6, 0, null, null)
    	[...]
    
   	!!! Note: 
		* See <a href="../reference/disk-store-utilities/dataextractloader.html#reference_13F8B5AFCD9049E380715D2EF0E33BDC" class="xref" title="Takes the SQL, CSV, and recommendations output files from dataextractor, and uses those inputs to load recovered disk store data into a new SnappyData system">dataextractloader</a> for a full description of additional command-line options.</p> 

        * Any errors that occur while loading data from the CSV files is recorded in the output log file, which is stored in <span class="ph filepath">EXTRACTED\_LOADER/extractor.log</span>. Errors do not prevent the loader from attempting to load further data.</p>

3.  Connect to the distributed system and verify that the recovered data was loaded:

    	$ snappy
        snappy> connect client 'localhost:1527';
        snappy> show tables;
        TABLE_SCHEM         |TABLE_NAME                    |REMARKS             
        ------------------------------------------------------------------------
        SYS                 |ASYNCEVENTLISTENERS           |                    
        SYS                 |GATEWAYRECEIVERS              |                    
        SYS                 |GATEWAYSENDERS                |                    
        SYS                 |SYSALIASES                    |                    
        SYS                 |SYSCHECKS                     |                    
        SYS                 |SYSCOLPERMS                   |                    
        SYS                 |SYSCOLUMNS                    |                    
        SYS                 |SYSCONGLOMERATES              |                    
        SYS                 |SYSCONSTRAINTS                |                    
        SYS                 |SYSDEPENDS                    |                    
        SYS                 |SYSDISKSTORES                 |                    
        SYS                 |SYSFILES                      |                    
        SYS                 |SYSFOREIGNKEYS                |                    
        SYS                 |SYSHDFSSTORES                 |                    
        SYS                 |SYSKEYS                       |                    
        SYS                 |SYSROLES                      |                    
        SYS                 |SYSROUTINEPERMS               |                    
        SYS                 |SYSSCHEMAS                    |                    
        SYS                 |SYSSTATEMENTS                 |                    
        SYS                 |SYSSTATISTICS                 |                    
        SYS                 |SYSTABLEPERMS                 |                    
        SYS                 |SYSTABLES                     |                    
        SYS                 |SYSTRIGGERS                   |                    
        SYS                 |SYSVIEWS                      |                    
        SYSIBM              |SYSDUMMY1                     |                    
        APP                 |AIRLINES                      |                    
        APP                 |CITIES                        |                    
        APP                 |COUNTRIES                     |                    
        APP                 |FLIGHTAVAILABILITY            |                    
        APP                 |FLIGHTS                       |                    
        APP                 |FLIGHTS_HISTORY               |                    
        APP                 |MAPS                          |                    

    	32 rows selected
    

    The above output shows that tables in the APP schema were recreated during the recovery process. Further queries against the tables show that the example data was also loaded.

<a id="topic_cqs_k5c_24"></a>

## Troubleshooting Data Recovery Errors

This section describes some common errors that can occur while recovering data or loading recovered data into a new system.

<a id="topic_cqs_k5c_24__table_bjw_s5c_24"></a>

| Error | Description |
|--------|--------|
|Errors indicate that a disk store was not recovered from a directory.|A common error during data extraction indicates that a named disk store was not recovered from a specific directory. This generally does not indicate an error in the extraction process. In order to avoid problems caused by corrupt directory mappings in oplog files, the utility looks for all disk store files in all directories listed for a SnappyData member. While this ensures that the tool recovers as much data as possible, it also results in this error when a disk store's files do not appear in a specified directory.|
|`dataextractor` fails to recover any data.|The persistent data dictionary must be available in order to recover any data from the disk store files. See <a href="#topic_eks_mxw_d4" class="xref" title="The following procedures and resources are required in order to use the data recovery utilities.">Requirements</a> |
|Out of Memory Exceptions during data recovery.|The `dataextractor` utility attempts to calculate the size of the target disk stores, and spawns multiple threads in order to extract data as fast as possible. The number of threads is determined by how much heap memory you provide to the utility. If you receive out of memory exceptions:</p> * Use the `--num-threads` option to reduce the number of threads spawned by the utility. Specify a value that is less than what `dataextractor` reports in the console when the extraction process begins.</br> * If the number of threads is already low, providing the utility with additional heap space.  |
|  Out of disk space errors.| If you run out of disk space while executing <code class="ph codeph">dataextractor</code>, the utility exits and all data that was recovered up to that point is available in the output directory. However, the <span class="ph filepath">Recommended.txt</span> and <span class="ph filepath">Summary.txt</span> files are not created. If this occurs, free the available disk space and then re-run the utility.|
|  Data not recovered for a server, "This oplog is a pre 7.0 version" error, or other failures.| A corrupted disk store metadata file (<span class="ph filepath">.if</span> extension) can result in a failure to extract data for a member, or can manifest itself in other ways, such as by reporting the &quot;pre 7.0 version&quot; error. In this case, data may not be recoverable unless you can restore a viable <span class="ph filepath">.if</span> file from backup.|
|Errors while loading recovered data.| As described in <a href="#id_vnc_ysw_d4" class="xref" title="The dataextractor utility provides only a &quot;best effort&quot; attempt to recover disk store data. Keep these limitations in mind when you use the utilities">Limitations for Data Recovery</a>, the disk store recovery process cannot guarantee data consistency. Errors that occur while loading recovered data are common. However, errors that occur while executing the <a href="../reference/disk-store-utilities/dataextractloader.html#reference_13F8B5AFCD9049E380715D2EF0E33BDC" class="xref noPageCitation" title="Takes the SQL, CSV, and recommendations output files from dataextractor, and uses those inputs to load recovered disk store data into a new SnappyData system">dataextractloader</a> do not prevent the utility from attempting to load additional data. See the <code class="ph codeph">dataextractloader</code> log file for a complete record of errors that occurred.|

