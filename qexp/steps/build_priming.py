import csv
import itertools as it
import logging
import os
import pickle
import typing
from pathlib import Path

from qexp import Config, DataCache
from qexp.extractors import WikipediaRevisionExtractor, WikitextExtractor
from qexp.extractors.Sampler import Sampler
from qexp.extractors.WikipediaExtractor import PageLinkExtractor
from qexp.Pipeline import Pipeline


def get_article_ids(filepath: str) -> typing.Iterator[str]:
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            yield record["article_id"]


def get_parent_titles(filepath: str) -> typing.Iterator[str]:
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for record in reader:
            yield record["parent_title"]
            next(reader)


class Flattener(object):
    def __init__(self):
        pass

    def run(
        self, xs: typing.List[typing.Tuple[str, typing.List[str]]]
    ) -> typing.List[str]:
        return it.chain.from_iterable([vs for _, vs in xs])


def main(args):
    logger = logging.getLogger("main")

    logger.info("Building priming...")

    config = args.config

    pipeline = Pipeline.make_pipeline(
        PageLinkExtractor(
            url="https://en.wikipedia.org/w/api.php",
            cache=DataCache(config.get("data", "links_path")),
            exclude_list=list(get_article_ids(config.get("data", "article_path"))),
            no_continue=True,
        ),
        Sampler(
            sample_size=5,
        ),
        Flattener(),
        WikipediaRevisionExtractor(
            url=config.get("wikipedia", "endpoint"),
            cache=DataCache(config.get("data", "extracts_path")),
        ),
        WikitextExtractor(
            cache=DataCache(config.get("data", "clean_path")),
        ),
    )

    _ = pipeline.run(get_parent_titles(config.get("data", "article_path")))

    with open(config.get("data", "links_dump"), "wb") as f:
        pagelinks_dict = {title: links for title, links in pipeline.steps[1][1]}
        pickle.dump(pagelinks_dict, f)

    return os.EX_OK
