#!/bin/bash
set -x #echo on

/etc/init.d/nginx stop
/usr/local/nginx/sbin/nginx -c /etc/nginx/nginx.conf
sleep 1
curl -o partial.car -H "Accept: application/vnd.ipld.car" -m 5  http://127.0.0.1:8080/fixture.car?bytes=0:1024
ls -lh partial.car
/usr/local/bin/car ls -v partial.car
sleep 1
cat /var/log/nginx/error.log
