#!/usr/bin/env python

# We still need a setup.py shim so we can still pip install -e .
# https://snarky.ca/what-the-heck-is-pyproject-toml/#how-to-use-pyproject-toml-with-setuptools

import setuptools

if __name__ == "__main__":
    setuptools.setup()