#!/usr/bin/env bash

function absPath() {
  perl -MCwd -le 'print Cwd::abs_path(shift)' "$1"
}

sbin="$(dirname "$(absPath "$0")")"

. "$sbin/common.funcs"

if [ -z "$1" ]; then
  echo "At least one user name must be provided"
  echo "Usage: `basename $0` <user1> <user2> ..."
  exit 1
fi

trap "stty echo; exit $?" EXIT

# get the plain-text passwords for all specified users
declare -a users
declare -a passwords
for user in "$@"; do
  while /bin/true; do
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
      echo
    else
      break
    fi
  done
  userIndex=$(keyPutIndex "$user" "${users[@]}")
  users[$userIndex]="$user"
  passwords[$userIndex]="$passwd1"
done

# get locator host:port
hostPort="$($sbin/snappy-locators.sh start -dump-server-info)"

# check for no specified client-port which will default to 1527 as per product defaults
clientPort=""
case "${hostPort}" in
  *:) clientPort="-client-port=1527"; hostPort="${hostPort}1527" ;;
esac

tmpOut="$(mktemp)"
ENCRYPT_PASSWORD_OPTIONS="-user=app -auth-provider=NONE -J-Dgemfirexd.thrift-default=false -log-level=warning $clientPort"
export ENCRYPT_PASSWORD_OPTIONS
$sbin/snappy-locators.sh start 2>&1 | tee "${tmpOut}"

locatorStarted="$(grep 'SnappyData Locator pid: .* status: running' "${tmpOut}")"
rm -f "${tmpOut}"

user=
passwd=
callStr=

for index in ${!users[@]}; do
  user="${users[$index]}"
  passwd="${passwords[$index]}"
  callStr="${callStr} call sys.encrypt_password('$user', '$passwd', 'AES', 0);"
done

if [ -n "${locatorStarted}" ]; then

# connect to temporary locators using DRDA
$sbin/../bin/snappy << EOF
connect 'jdbc:snappydata:drda://$hostPort/;load-balance=false';
$callStr
EOF

$sbin/snappy-locators.sh stop

else

# connect to existing cluster using default thrift
$sbin/../bin/snappy << EOF
connect 'jdbc:snappydata://$hostPort/;user=$user;password=$passwd';
$callStr
EOF

fi
