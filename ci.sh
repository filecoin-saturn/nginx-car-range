#!/bin/bash

/usr/local/nginx/bin/nginx &
sleep 1
curl http://127.0.0.1/fixture.car?bytes=0:1024
