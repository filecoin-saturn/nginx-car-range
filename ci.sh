#!/bin/bash
set -x #echo on

/etc/init.d/nginx stop
/usr/local/nginx/sbin/nginx -c /etc/nginx/nginx.conf
sleep 1

curl -o partial.car -s -w "time: %{time_total} s\n" -H "Accept: application/vnd.ipld.car" -m 5  http://127.0.0.1:8080/fixture.car
ls -lh partial.car
/usr/local/bin/car ls -v partial.car

curl -o partial.car -s -w "time: %{time_total} s\n" -H "Accept: application/vnd.ipld.car" -m 5  http://127.0.0.1:8080/fixture.car?bytes=0:262144
ls -lh partial.car
/usr/local/bin/car ls -v partial.car

curl -o partial.car -s -w "time: %{time_total} s\n" -H "Accept: application/vnd.ipld.car" -m 5  http://127.0.0.1:8080/fixture.car?bytes=262144:524288
ls -lh partial.car
/usr/local/bin/car ls -v partial.car

sleep 1
cat /var/log/nginx/error.log
