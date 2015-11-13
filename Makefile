#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#

#
# Copyright (c) 2015, Joyent, Inc.
#

#
# Tools
#
TAPE			:= ./node_modules/.bin/tape
NPM			:= npm

#
# Files
#
JS_FILES	:= manatee.js test/client.test.js
JSL_CONF_NODE	 = tools/jsl.node.conf
JSL_FILES_NODE	 = $(JS_FILES)
JSSTYLE_FILES	 = $(JS_FILES)
JSSTYLE_FLAGS	 = -f tools/jsstyle.conf

CLEAN_FILES	+= node_modules

include ./tools/mk/Makefile.defs

#
# Repo-specific targets
#
.PHONY: all
all: $(TAPE) $(REPO_DEPS)
	$(NPM) rebuild

$(TAPE): | $(NPM_EXEC)
	$(NPM) install


.PHONY: test
test: $(TAPE)
	node test/client.test.js

include ./tools/mk/Makefile.deps
include ./tools/mk/Makefile.targ
