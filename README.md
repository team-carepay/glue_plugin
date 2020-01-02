# Glue Airlfow plugin

This repository contains a plugin to help with managing Glue via Airflow.

## Local development

We use [pre-commit](https://pre-commit.com/) as a method of locally verifying code quality. pre-commit installs a set of git hooks locally.

To get started initialize a virtualenv and install the local requirements:

```console
python3 -m venv ./env
. ./env/bin/activate
pip install -r requirements-dev.txt
```

The git hooks can then be installed with pre-commit:

```console
pre-commit install
```
