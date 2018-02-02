# Add the default user/group

SNAPPY_USER=snappydata
SNAPPY_GROUP=snappydata
SNAPPY_HOME=/opt/$SNAPPY_USER

[ "$DPKG_MAINTSCRIPT_PACKAGE" ] && quiet="--quiet"

if ! getent group $SNAPPY_GROUP > /dev/null; then
   addgroup --system $quiet $SNAPPY_GROUP
fi
if ! getent passwd $SNAPPY_USER > /dev/null; then
  adduser --system $quiet --home $SNAPPY_HOME --no-create-home --shell /bin/bash \
      --ingroup $SNAPPY_GROUP --gecos "SnappyData cluster owner" $SNAPPY_USER
fi
