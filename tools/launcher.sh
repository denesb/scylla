#!/bin/bash

path=$(dirname $0)
bin=$(basename $0)

exec -a $bin ${path}/../scylla "$@"
