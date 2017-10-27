# Add the default user/group

SNAPPY_USER=snappydata
SNAPPY_GROUP=snappydata
SNAPPY_HOME=/opt/$SNAPPY_USER

if ! getent group $SNAPPY_GROUP > /dev/null; then
  groupadd -r $SNAPPY_GROUP
fi
if ! getent passwd $SNAPPY_USER > /dev/null; then
  useradd -r -M -d $SNAPPY_HOME -s /bin/bash -N -g $SNAPPY_GROUP \
      -c "SnappyData cluster owner" $SNAPPY_USER
fi

# remove old system profiles if present
rm -f /opt/$SNAPPY_HOME/profile.d/snappydata.*sh
