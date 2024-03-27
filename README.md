# PSQE: Personalized Semantic Query Expansion for user-centric query disambiguation

This repository provides the code required to replicate the experiments documented in the paper.

## Requirements

* Python 3.11
* [poetry](https://python-poetry.org/)
* Docker

## Getting started

### Dependencies

You will need to have [poetry](https://python-poetry.org/) set up and running.
Then, in this project-dir, run:

```shell
$ poetry install
```

This should create a project-local `.venv` with all dependencies.

### Make

The Makefile provides targets for all steps leading up to and including the final experiments:

```shell
$ make spotlight # spin up DBPedia Spotlight
# then, in a different terminal:
$ make all
```

It may happen that `make all` runs into errors with the Wikipedia API.
In that case, simply run it again, most data is cached and won't be reloaded.