import argparse
import csv
import logging
import os
from pathlib import Path

import igraph as ig

from qexp import Config
from qexp.builder import KnowledgeGraphBuilder


def get_article_ids(filepath: str):
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            yield record["article_id"]


def main(args: argparse.Namespace):
    config = args.config
    logger = logging.getLogger("main")

    logger.info("Building knowledge graph...")

    articles = list(get_article_ids(config.get("data", "article_path")))

    builder = KnowledgeGraphBuilder(config)
    graph = builder(articles)
    ig.Graph.write_pickle(graph, config.get("data", "kg_path"))

    return os.EX_OK
