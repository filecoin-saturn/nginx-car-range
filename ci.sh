#!/bin/bash

/usr/local/nginx/sbin/nginx &
sleep 1
curl http://127.0.0.1/fixture.car?bytes=0:1024
cat /var/log/nginx/error.log
