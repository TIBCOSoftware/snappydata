#!/bin/bash
echo "usage: ./gen.sh [cc] [d] <mapperfile> <numrows> <tablenames>"

if [[ $1 = 'cc' ]]; then
 echo Compiling .. 
 rm *.class
ls -l;
 javac DataGenerator_.java
 shift;
fi

if [[ $1 = 'd' ]]; then
 DBG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=localhost:1044"
 shift;
else
 DBG=
fi

  _SQ=$SNAPPY_HOME/build-artifacts/scala-2.11/store/lib


mapper=${1:-mapper}
numrows=${2:-5}
tabName=${3:-APP.PNP_SUBSCRIPTIONS}

echo "gen.sh using $_SQ is generating $numrows rows using $mapper for $tabName"
echo java  -Xms2g -Xmx2g  $DBG -cp .:$_SQ/snappydata-client-1.5.3.jar DataGenerator_ $tabName localhost:1527 $numrows $mapper
java  -Xms2g -Xmx2g  $DBG -cp .:$_SQ/snappydata-client-1.5.3.jar DataGenerator_ $tabName localhost:1527 $numrows $mapper

