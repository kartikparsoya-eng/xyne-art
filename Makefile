# Xyne ART — canonical entry points. See README.md and ART.md.
PY      := .venv/bin/python
PIP     := .venv/bin/pip
RUFF    := .venv/bin/ruff
REPORTS := reports

.PHONY: venv deps test lint check clean clean-reports help

help:  ## show this help
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

venv:  ## create .venv and install runtime + dev deps
	python3 -m venv .venv
	$(PIP) install -r requirements.txt -r requirements-dev.txt

deps:  ## (re)install deps into an existing venv
	$(PIP) install -r requirements.txt -r requirements-dev.txt

test:  ## run the unit test suite (no live server needed)
	[ -x $(PY) ] || $(MAKE) venv
	$(PY) -m pytest tests/ -q

lint:  ## ruff check
	[ -x $(RUFF) ] || $(PIP) install -q ruff==0.12.8
	$(RUFF) check .

check: lint test  ## lint + test

clean-reports:  ## remove generated reports (regenerated per rig)
	rm -rf $(REPORTS)/*

clean: clean-reports  ## remove reports + python caches
	find . -type d -name __pycache__ -not -path './.venv/*' -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache
