import csv
import itertools as it
import logging
import os
import pickle

import igraph as ig

from qexp import DataCache
from qexp.builder import ProfileBuilder
from qexp.extractors import WikipediaRevisionExtractor, WikitextExtractor
from qexp.extractors.SparqlExtractor import WikiPageIdExtractor
from qexp.Pipeline import Pipeline


def get_article_ids(filepath: str):
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            yield record["article_id"]


def main(args):
    logger = logging.getLogger("main")

    logger.info("Building profiles...")

    config = args.config

    graph = ig.Graph.Read_Pickle(config.get("data", "kg_path"))
    articles = list(get_article_ids(config.get("data", "article_path")))

    builder = ProfileBuilder()
    profiles = builder(graph, articles)

    extractor = WikiPageIdExtractor(endpoint=config.get("dbpedia", "sparql_url"))

    profiles = extractor.run(profiles)

    pipeline = Pipeline.make_pipeline(
        WikipediaRevisionExtractor(
            url=config.get("wikipedia", "endpoint"),
            cache=DataCache(config.get("data", "extracts_path")),
        ),
        WikitextExtractor(
            cache=DataCache(config.get("data", "clean_path")),
        ),
    )

    # doing this for the side effect of persisting articles to disk only
    _ = pipeline.run(it.chain.from_iterable([steps for _, steps in profiles]))

    profiles = {k: vs for (k, vs) in profiles}

    with open(config.get("data", "profiles_dump"), "wb") as f:
        pickle.dump(profiles, f)

    return os.EX_OK
