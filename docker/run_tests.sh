#!/usr/bin/env bash

set -e

eval "$(pyenv init -)"

pip install -r requirements.txt -r requirements-test.txt
rm -rf tests/*/__pycache__
export DB="aerospike://"
pytest integration_tests/test_dagger.py --cov-report="html:/tmp/coverage_integation"
