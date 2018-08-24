#!/usr/bin/env bash

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}

sbin="$(dirname "$(absPath "$0")")"

if [ -z "$1" ]; then
  echo "At least one user name must be provided"
  echo "Usage: `basename $0` <user1> <user2> ..."
  exit 1
fi

trap "stty echo; exit $?" EXIT

# get the plain-text passwords for all specified users
declare -A userPasswords
for user in "$@"; do
  echo -n "Enter password for $user: "
  stty -echo
  read passwd1
  stty echo
  echo
  echo -n "Re-enter password for $user: "
  stty -echo
  read passwd2
  stty echo
  echo
  if [ "${passwd1}" != "${passwd2}" ]; then
    echo Passwords for $user do not match
    exit 1
  fi
  userPasswords["${user}"]="${passwd1}"
done

# get locator host:port
hostPort="$($sbin/snappy-locators.sh start -dump-server-info)"

# check for no specified client-port which will default to 1527 as per product defaults
clientPort=""
case "${hostPort}" in
  *:) clientPort="-client-port=1527"; hostPort="${hostPort}1527" ;;
esac

$sbin/snappy-locators.sh start -user=app -auth-provider=NONE -J-Dgemfirexd.thrift-default=false -log-level=warning "$clientPort"

callStr=
for user in ${!userPasswords[@]}; do
  callStr="${callStr} call sys.encrypt_password('$user', '${userPasswords[$user]}', 'AES', 0);"
done

$sbin/../bin/snappy << EOF
connect 'jdbc:snappydata:drda://$hostPort/;load-balance=false';
$callStr
EOF

$sbin/snappy-locators.sh stop
