------------------------------------------------------------------------------------------------------------------------

                                      How to launch the cluster upgrade script.

------------------------------------------------------------------------------------------------------------------------
Run the following command
    ./clusterUpgrade.sh <snappyBuildPath1> <snappyBuildPath2> ... <snappyBuildPathN>

The clusterUpgrade.sh script can be found inside dtests/src/resources/scripts/clusterUpgrade directory.
The script also includes a clusterUpgrade.conf  that contains different configurable parameters, the parameters are as
follows:

    snappydataDir = <Snappydata checkout path>
    snappydataTestDir = <Directory to launch the script from>
    resultDir = <Directory where the results will be stored>
    jarFile = <Test jar path>

