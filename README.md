# nginx-car-range

> Nginx plugin for filtering range requests from CAR files

## Functionality

This is a plugin for nginx. The functionality is conceptually very
similar to http range request handling, but with some IPLD-specific
semantics to allow clients to perform incremental validation of responses.

### Activation

The plugin will only act on requests meeting the following conditions:

* The 'Accept' header matches 'application/vnd.ipld.car'
* A query parameter is set of the form 'entity-bytes=x:y'

### Behavior

The plugin will trigger a sub-request without the query parameter for `entity-bytes`.

On the Car archive that is returned it will then filter some 'blocks'
and end the response early based on the bytes requested.

* When a block has a 'dagpb' codec, it will attemp to decode the data
  field as a unixfs protobuf. If it succeeds, it will build a map of
  the byte offsets of blocks in the file to identify the blocks at the
  beginning and ending of the requested range.
* When blocks are seen after this map is constructed which fall
  outside of the requested range, they will be discarded. (this rule
  skips responses up to the beginning of the range)
* When a block at the beginning of the range is seen, the start of the
  range pointer will be advanced
* When the start of range pointer reaches the end of range pointer, the
  response will be terminated as successful.

When data is seen that does not match a unixfs file, no filtering
will occur, as the range query has undefiend bahavior in other scenarios.

## Installation

A plugin artifact is produced in the docker build environment that may
be linked into nginx using the `load_module` directive.

## Building

This module generates unixfs protobufs and nginx bindings at build time. If you want to build it locally, make sure you have prost installed and nginx.

By default it will look for nginx directory at `../nginx` or you can set the repo path by exporting `NGINX_DIR=<path-to-nginx>` so it can find the relevant C headers.

## License

Apache-2.0/MIT © Protocol Labs
