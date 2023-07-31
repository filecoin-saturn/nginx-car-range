#!/bin/bash
set -x #echo on

test_range_request () {
  range="$1"
  name="$2"
  code="$(curl -sw "%{http_code}\n" -o partial.car -H "Accept: application/vnd.ipld.car" "http://127.0.0.1:8080/${name}.car?entity-bytes=${range}")"
  test "$code" -eq 200 || (cat /var/log/nginx/error.log && exit 1)
  ls -l partial.car
  /usr/local/bin/car ls -v partial.car
}

/etc/init.d/nginx stop
/usr/local/nginx/sbin/nginx -c /etc/nginx/nginx.conf
sleep 1

# curl -o partial.car -s -w "time: %{time_total} s\n" -H "Accept: application/vnd.ipld.car" -m 5  http://127.0.0.1:8080/fixture.car
# ls -lh partial.car
# /usr/local/bin/car ls -v partial.car

test_range_request "0:1048576" "midfixture"

test_range_request "0:1048576" "bigfixture"

test_range_request "1048576:2097152" "midfixture"

test_range_request "555555:999999" "bigfixture"

test_range_request "1048576:2097151" "fixture3"

# try a cache hit
test_range_request "1048576:2097151" "fixture3"

cat /var/log/nginx/error.log
