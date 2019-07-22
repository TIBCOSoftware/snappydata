-----------------------------------------------------------------------------------------------------------------------

                                      How to launch the cluster upgrade script.

-----------------------------------------------------------------------------------------------------------------------
The clusterUpgrade.sh script can be found inside dtests/src/resources/scripts/clusterUpgrade directory.
The script also includes a clusterUpgrade.conf  that contains different configurable parameters
In order to execute the clusterUpgrade.sh script, you need to first set the snappydataDir parameter in the
clusterUpgrade.conf file to point to your latest SnappyData checkout and also the resultDir.
The parameters are as follows:

    snappydataDir=<Latest SnappyData checkout path>
    resultDir=<Directory where the results will be stored>

After setting the above parameters in the  clusterUpgrade.conf file, execute the following:

Run the following command
    ./clusterUpgrade.sh <snappyBuildPath1> <snappyBuildPath2> ... <snappyBuildPathN>
