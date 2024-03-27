import argparse
import csv
import os
from pathlib import Path

from qexp.expand import ModelRegistry
from qexp.experiments.searcher import Searcher
from qexp.experiments.types import Result, SearchSpecsBuilder
from qexp.search import Search


def main(args: argparse.Namespace):
    config = args.config
    model_path = args.model_path
    results_path = args.results_path

    search = Search(collection=Path(config.get("data", "clean_path")).glob("*"))
    registry = ModelRegistry(Path(model_path))

    search_specs = SearchSpecsBuilder(config.get("data", "search_config"))

    results = Searcher(search_specs, search, registry).run()

    with open(results_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(Result.HEADER)
        w.writerows(results)

    return os.EX_OK
