#!/bin/bash

set -o xtrace

git submodule update --init

cd ./deps/javascriptlint/
make install
cd ../..

./deps/javascriptlint/build/install/jsl --conf ./tools/jsl.node.conf \
    ./*.js ./bin/*.js ./test/*.js

./deps/jsstyle/jsstyle -f ./tools/jsstyle.conf ./*.js ./bin/*.js ./test/*.js
