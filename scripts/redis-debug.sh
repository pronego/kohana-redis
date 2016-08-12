#!/bin/bash

tmp_dir=/tmp/redis-debug
file="$(date +%s).lua"

mkdir -p ${tmp_dir}

lua-compose $1 > "$tmp_dir/$file"

if [ $? -eq 0 ]; then
	redis-cli --ldb --eval "$tmp_dir/$file" "${@:2}"
fi
