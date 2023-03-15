#!/bin/bash

/etc/init.d/nginx stop
/usr/local/nginx/sbin/nginx -c /etc/nginx/nginx.conf
sleep 1
curl -H "Accept: application/vnd.ipld.car" -m 5  http://127.0.0.1:8080/fixture.car?bytes=0:1024
sleep 1
cat /var/log/nginx/error.log
