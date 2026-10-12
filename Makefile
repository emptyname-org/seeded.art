CARGO ?= cargo
NPM ?= npm
TRADING_PYTHON ?= $(CURDIR)/tmp/trading/venv/bin/python
export PATH := $(CURDIR)/tmp/tooling/bin:$(PATH)
export CARGO_HOME ?= $(CURDIR)/tmp/cargo-home
export CARGO_TARGET_DIR ?= $(CURDIR)/target
export TMPDIR := $(CURDIR)/tmp
export PYTHONDONTWRITEBYTECODE := 1
export PYTHONPATH := $(CURDIR)/tmp/deployment_tools

.PHONY: build test trading-tools browser-tools browser-test proxy-fixture lint audit run clean

build: browser-tools
	node assets/static/beacon/build.mjs
	mkdir -p tmp
	$(CARGO) build --release --locked --bins
	python3 deploy/package_release.py --compiled "$(CARGO_TARGET_DIR)/release"

test: proxy-fixture
	mkdir -p tmp
	$(CARGO) test --locked --all-targets
	SEEDED_REQUIRE_NATIVE_PROXY=1 python3 -m unittest discover -s deploy -p 'test_*.py'
	$(TRADING_PYTHON) tests/trading_contracts.py
	$(MAKE) browser-test

trading-tools:
	python3 -m venv tmp/trading/venv
	tmp/trading/venv/bin/pip install --cache-dir tmp/pip_cache pytezos==3.18.0

browser-tools:
	mkdir -p tmp/browser_tools
	cp package.json package-lock.json tmp/browser_tools/
	$(NPM) ci --prefix tmp/browser_tools --ignore-scripts --no-audit --cache tmp/npm_cache

browser-test: browser-tools proxy-fixture
	node assets/static/beacon/build.mjs
	node tests/browser/player.mjs
	node tests/browser/auth.mjs
	node tests/browser/market.mjs
	node tests/browser/load_more.mjs
	node tests/browser/theme.mjs

proxy-fixture:
	$(CARGO) build --locked --example proxy_fixture

lint:
	mkdir -p tmp
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --locked --all-targets -- -D warnings
	python3 -m ruff check --cache-dir tmp/ruff_deployment deploy tests/trading_contracts.py
	python3 -m ruff format --check --cache-dir tmp/ruff_deployment deploy tests/trading_contracts.py
	python3 -m mypy --strict --cache-dir tmp/mypy_deployment deploy/configure.py deploy/measure_state.py deploy/package_release.py deploy/maintain.py deploy/forum_backup.py deploy/monitor.py deploy/indexnow.py tests/trading_contracts.py
	$(MAKE) audit

audit:
	mkdir -p tmp
	$(NPM) audit --prefix tmp/browser_tools --cache tmp/npm_cache
	$(CARGO) audit --deny warnings --db tmp/advisory-db
	$(CARGO) tree --locked

run: browser-tools
	node assets/static/beacon/build.mjs
	mkdir -p tmp
	$(CARGO) run --locked -- dev

clean:
	$(CARGO) clean
