SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

py := $$(if [ -d '.venv' ]; then echo ".venv/bin/python"; else echo "python"; fi)
pip := $(py) -m pip

.DEFAULT_GOAL := help
.PHONY: help
help: ## Display this help section
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z\$$/]+.*:.*?##\s/ {printf "\033[36m%-38s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: all
all: services data/experiment_1.csv data/experiment_2.csv ## Run all experiments

.PHONY: spotlight
spotlight: spotlight-compose.yaml ## IMPORTANT: wait for this to spin up completely; it downloads the spotlight model, which may take some time
	docker-compose \
		--file ./spotlight-compose.yaml \
		up

.PHONY: services
services: docker-compose.yaml ## Spin up dependent services via docker-compose
	docker-compose \
		--file ./docker-compose.yaml \
		up --detach

data/graph.pickle: data/ambiguous_articles.csv ## Build the Wikipedia KG
	$(py) main.py \
		kg

data/ambiguous_pages_links.pickle: data/ambiguous_articles.csv ## Sample some links
	$(py) main.py \
		priming

data/_cleaned/* data/profiles.pickle data/_extracts/*: data/graph.pickle ## Build profiles via random walks
	$(py) main.py \
		profiles

results/user_categories.csv results/article_categories.csv: data/_extracts/* ## Aggregate the category labels
	$(py) main.py \
		categories

models/*.model: data/profiles.pickle ## Build word2vec models of profiles
	$(py) main.py \
	 	embeddings \
		--tfidf=0.00 \
		models/ \
		data/profiles.pickle

data/search_test.csv: data/ambiguous_articles.csv ## Build search-configuration for experiments
	$(py) main.py \
	 	searchconfig \

data/experiment_1.csv: models/*.model data/search_test.csv data/ambiguous_pages_links.pickle ## Run experiment 1 ("SMALL" in the paper)
	$(py) main.py \
	 	 experiment1 \
		--model-path=./models \
		--results-path=results/experiment_1.csv

data/experiment_2.csv: models/*.model data/search_test.csv ## Run experiment 2 ("LARGE" in the paper)
	$(py) main.py \
	 	 experiment2 \
		--model-path=./models \
		--results-path=results/experiment_2.csv