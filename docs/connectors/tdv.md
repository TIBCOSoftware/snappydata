# Connecting to TDV as Data Source from TIBCO ComputeDB

## Overview

TIBCO Data Virtualization (TDV) is an enterprise data virtualization solution that orchestrates access to multiple and varied data sources and delivers the datasets and IT-curated data services foundation for nearly any solution.
ComputeDB can connect to TDV as a data source to import, process, and store data.

## Version and Compatibility

TIBCO ComputeDB edition 1.2.0 is tested and works with TIBCO Data Virtualization 8.2.0 version.

## Connecting to TDV from TIBCO ComputeDB

1.	Publish the view from TDV Studio that needs to be accessed as a data source from TIBCO ComputeDB. 
2.	Note the name of the Data Source that is associated with the View that should be published. For more information, refer to the TDV documentation.
3.	Log on to the machine where TIBCO ComputeDB is installed and go to the ComputeDB installation directory.

4.	Make sure ComputeDB is running by executing the following command:	

            ./sbin/snappy-status-all.sh

5.	Launch the TIBCO ComputeDB SQL shell:

			./bin/snappy

6.	Connect to the running TIBCO ComputeDB cluster:

			snappy>connect client '<host-name>:<locator port>';
			For example: snappy>connect client '<localhost>:<1527>';
7.	Deploy the JDBC jar of the TDV module:

			snappy>deploy jar <jar alias> '<ComputeDB installation directory>/connectors/csjdbc8.jar';
            
            For example:
            snappy>deploy jar dv-jar '/opt/tib_computedb_TIB_compute_1.2.0_linux/connectors/csjdbc8.jar';
	
    !!!Note
    	This command should be executed only once, when you connect to the TDV cluster for the first time.

8.	Create an external table with JDBC options:

           CREATE  external TABLE <table-name> USING jdbc OPTIONS (dbtable '<View name in TDV studio>', driver 'cs.jdbc.driver.CompositeDriver',  user '<TDV user>',  password '<TDV password>',  url 'jdbc:compositesw:dbapi@<TDV hostname>:<Port>?domain=composite&dataSource=<TDV Data source>');

          For example:
          CREATE  external TABLE ext_tdv USING jdbc OPTIONS (dbtable 'CompositeView', driver 'cs.jdbc.driver.CompositeDriver',  user 'admin',  password '<TDV password>',  url 'jdbc:compositesw:dbapi@tdv-host:9401?domain=composite&dataSource=DV-DS');

9.	Run the required SQL queries on the created external table.


If both, TDV Studio and TIBCO ComputeDB are installed on different machines, ensure the machine on which TDV Studio is hosted, is accessible from the machine on which TIBCO ComputeDB is installed. If TDV is installed on a Windows machine, ensure to create a rule in the Firewall settings to allow the inbound traffic for all ports from the machine on which TIBCO ComputeDB is installed. The following steps ow detail the procedure to do this on Windows Server 2012 edition:

Go to Windows Firewall with Advanced Security -> New rule.
Enter / select the following values:
For the ‘type of rule’, select Port. Click Next.
For ‘Does this rule apply to TCP or UDP’, select TCP.
For ‘Does this rule apply to all local ports or specific local ports’, select All local ports.
For ‘What action should be taken…..’, select Allow the connection.
For ‘When does this rule apply?’, select all the options.
Enter appropriate name for the rule and click on the Finish button.



      