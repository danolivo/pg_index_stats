#!/bin/bash
ulimit -c unlimited

# Kill all orphan processes
pkill -U `whoami` -9 -e postgres
pkill -U `whoami` -9 -e pgbench
pkill -U `whoami` -9 -e psql

sleep 1

M=`pwd`/PGDATA
U=`whoami`

rm -rf $M || true
mkdir $M
rm -rf logfile.log || true

export LC_ALL=C
export LANGUAGE="en_US:en"
initdb -D $M --locale=C

# PG Version-specific settings
ver=$(pg_ctl -V | egrep -o "[0-9]." | head -1)
echo "PostgreSQL version: $ver"
if [ $ver -gt 13 ]
then
  echo "compute_query_id = 'regress'" >> $M/postgresql.conf
fi

pg_ctl -w -D $M -l logfile.log start
createdb $U

