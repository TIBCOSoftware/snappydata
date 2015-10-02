#!/bin/sh

if [ "$1" = "-u" ]; then
  find . -name build.gradle | xargs perl -pi -e "s,^//(.*[sS]cala['\.]),\$1,"
else
  find . -name build.gradle | xargs perl -pi -e "s,^(.*[sS]cala['\.]),//\$1,"
fi
