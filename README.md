# nginx-car-range

> Nginx plugin for filtering range requests from CAR files

## Usage

This module generates unixfs protobufs and nginx bindings at build time. If you want to build it locally, make sure you have prost installed and nginx.

By default it will look for nginx directory at `../nginx` or you can set the repo path by exporting `NGINX_DIR=<path-to-nginx>` so it can find the relevant C headers.
