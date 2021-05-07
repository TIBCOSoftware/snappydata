# Connecting to TDV as Data Source from SnappyData

## Overview

TIBCO Data Virtualization (TDV) is an enterprise data virtualization solution that orchestrates access to multiple and varied data sources and delivers the datasets and IT-curated data services foundation for nearly any solution.
SnappyData can connect to TDV as a data source to import, process, and store data.

## Version and Compatibility

SnappyData edition 1.3.0 is tested and works with TIBCO Data Virtualization 8.2.0 version.

## Connecting to TDV from SnappyData

1.	Publish the view from TDV Studio that needs to be accessed as a data source from SnappyData. 
2.	Note the name of the Data Source that is associated with the view that should be published. For more information, refer to the TDV documentation.
3.	Log on to the machine where SnappyData is installed and go to the SnappyData installation directory.

4.	Make sure SnappyData is running by executing the following command:	

            ./sbin/snappy-status-all.sh

5.	Launch the SnappyData SQL shell:

			./bin/snappy

6.	Connect to the running SnappyData cluster:

			snappy>connect client '<host-name>:<locator port>';
			For example: snappy>connect client '<localhost>:<1527>';
7.	Deploy the JDBC jar of the TDV module:

			snappy>deploy jar <jar alias> '<SnappyData installation directory>/connectors/csjdbc8.jar';
            
            For example:
            snappy>deploy jar dv-jar '/snappydata/snappy-connectors/tdv-connector/lib/csjdbc8.jar';
	
    !!!Note
    	You should execute this command only once when you connect to the TDV cluster for the first time.

8.	Create an external table with JDBC options:

           CREATE  external TABLE <table-name> USING jdbc OPTIONS (dbtable '<View name in TDV studio>', driver 'cs.jdbc.driver.CompositeDriver',  user '<TDV user>',  password '<TDV password>',  url 'jdbc:compositesw:dbapi@<TDV hostname>:<Port>?domain=composite&dataSource=<TDV Data source>');

          For example:
          CREATE  external TABLE ext_tdv USING jdbc OPTIONS (dbtable 'CompositeView', driver 'cs.jdbc.driver.CompositeDriver',  user 'admin',  password '<TDV password>',  url 'jdbc:compositesw:dbapi@tdv-host:9401?domain=composite&dataSource=DV-DS');

9.	Run the required SQL queries on the created external table.


If you install both TDV Studio and SnappyData on different machines, ensure the machine on which TDV Studio is hosted, is accessible from the machine on which SnappyData is installed. If TDV is installed on a Windows machine, ensure to create a rule in the Firewall settings. This rule should allow the inbound traffic for all the ports from the machine on which SnappyData is installed. Use the following steps to do this on Windows Server 2012 edition:

1.	Go to Windows Firewall with Advanced Security -> New rule.
2.	Provide the following details:
	
    | Item | Option to Select |
	|--------|--------|
	|   **Type of rule**    |    Port   |
    |**Does this rule apply to TCP or UDP**|  TCP|
    |**Does this rule apply to all local ports or specific local ports**|  Select All local ports.|
    |**What action should be taken….**|Allow the connection|
    |**When does this rule apply?**| Select all the options|

3.	Enter the appropriate name for the rule and click on the Finish button.
