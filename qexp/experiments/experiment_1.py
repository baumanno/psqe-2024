import argparse
import collections
import csv
import logging
import os
import pickle
import pprint
import sys
import typing
from pathlib import Path

from qexp import Config
from qexp.expand import ModelRegistry
from qexp.experiments.searcher import Searcher
from qexp.experiments.types import Result, SearchSpecsBuilder
from qexp.search import Search


def get_ambiguous_articles(filepath: str) -> typing.Iterator[str]:
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            yield record["article"], record["article_id"]


def main(args):
    config = args.config
    model_path = args.model_path
    results_path = args.results_path

    splits = collections.defaultdict(set)
    with open(config.get("data", "search_config"), "r", newline="") as f:
        reader = csv.DictReader(f)
        for record in reader:
            splits[record["parent_title"]].add(record["article_id"])

    with open(config.get("data", "links_dump"), "rb") as f:
        link_samples = pickle.load(f)

    files_per_split: typing.DefaultDict[str, typing.Set[str]] = collections.defaultdict(
        set
    )
    for parent, articles in splits.items():
        files_per_split[parent].update(articles)
        files_per_split[parent].update(
            [link for link in link_samples[parent] if len(link) > 1]
        )

    registry = ModelRegistry(Path(model_path))

    if os.path.exists(results_path):
        os.unlink(results_path)

    with open(results_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(Result.HEADER)

    for parent_title, files in files_per_split.items():
        collection = [Path(config.get("data", "clean_path")) / file for file in files]

        search = Search(collection=collection)

        search_specs = SearchSpecsBuilder(config.get("data", "search_config"))
        search_specs = [
            spec for spec in search_specs if spec.query == parent_title.lower().strip()
        ]

        results = Searcher(search_specs, search, registry).run()

        with open(results_path, "a", newline="") as f:
            w = csv.writer(f)
            w.writerows(results)

    return os.EX_OK
