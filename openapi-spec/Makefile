PYTHON := .venv/bin/python
OPENAPI_GENERATOR := .venv/bin/openapi-generator-cli
UV := env UV_CACHE_DIR=/tmp/openapi-spec-uv-cache uv
SPEC := petstore-openapi.json
OUT := generated-python-client
PACKAGE_NAME := petstore_client
PROJECT_NAME := petstore-client
PACKAGE_VERSION := $(shell cat VERSION)

.PHONY: build bump-minor bump-patch clean generate-client generate-requirements install-deps prune-generated-tests test-generated validate

build: generate-client test-generated prune-generated-tests
	$(PYTHON) -m build -n

generate-client: clean validate
	$(OPENAPI_GENERATOR) generate \
		-g python \
		-i $(SPEC) \
		-o $(OUT) \
		--global-property apis,models,supportingFiles=api_client.py:configuration.py:exceptions.py:rest.py:api_response.py:__init__.py:py.typed \
		--additional-properties=packageName=$(PACKAGE_NAME),projectName=$(PROJECT_NAME),packageVersion=$(PACKAGE_VERSION),generateSourceCodeOnly=true \
		--skip-operation-example
	touch $(OUT)/$(PACKAGE_NAME)/py.typed
	$(MAKE) generate-requirements

generate-requirements:
	$(UV) pip compile pyproject.toml -o $(OUT)/requirements.txt

validate:
	$(OPENAPI_GENERATOR) validate -i $(SPEC)

test-generated:
	PYTHONPATH=$(OUT) $(PYTHON) -m unittest discover -s $(OUT)/$(PACKAGE_NAME)/test -p 'test_*.py'

prune-generated-tests:
	rm -rf $(OUT)/$(PACKAGE_NAME)/test

install-deps:
	$(PYTHON) -m pip install -r requirements.txt

clean:
	rm -rf $(OUT) dist build *.egg-info

bump-patch:
	$(PYTHON) scripts/bump_version.py patch

bump-minor:
	$(PYTHON) scripts/bump_version.py minor
