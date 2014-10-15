#!/bin/bash
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2014, Joyent, Inc.
#

set -o xtrace

git submodule update --init

cd ./deps/javascriptlint/
make install
cd ../..

./deps/javascriptlint/build/install/jsl --conf ./tools/jsl.node.conf \
    ./*.js ./test/*.js

./deps/jsstyle/jsstyle -f ./tools/jsstyle.conf ./*.js ./test/*.js
