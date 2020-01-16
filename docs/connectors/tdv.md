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
Make sure ComputeDB is running by executing the following command:	

            ./sbin/snappy-status-all.sh

4.	Launch the TIBCO ComputeDB SQL shell:

			./bin/snappy

4.	Connect to the running TIBCO ComputeDB cluster:

			snappy>connect client '<host-name>:<locator port>';
			For example: snappy>connect client '<localhost>:<1527>';
5.	Deploy the JDBC jar of the TDV module:

			snappy>deploy jar <jar alias> '<ComputeDB installation directory>/connectors/csjdbc8.jar';
            
            For example:
            snappy>deploy jar dv-jar '/opt/tib_computedb_TIB_compute_1.2.0_linux/connectors/csjdbc8.jar';
	
    !!!Note
    	This command should be executed only once, when you connect to the TDV cluster for the first time.
      