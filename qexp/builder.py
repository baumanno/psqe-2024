import itertools as it
import os
import pprint
import sys
import typing
from collections import defaultdict
from multiprocessing import Pool
from typing import List

import igraph

import qexp.util.types as mytypes
from qexp import Config, DataCache
from qexp.extractors import (
    SpotlightExtractor,
    SubjectExtractor,
    WikipediaCategorySampler,
    WikipediaRevisionExtractor,
    WikitextExtractor,
)
from qexp.Pipeline import Pipeline

KnowledgeGraph = igraph.Graph


class KnowledgeGraphBuilder(object):

    def __init__(self, config: Config):
        self.config = config

    @classmethod
    def to_edgelist(cls, xs: List[mytypes.PipelineResult]) -> List[mytypes.Edge]:
        return list(
            it.chain.from_iterable((zip(it.repeat(key), values) for key, values in xs))
        )

    def __call__(self, articles, **kwargs) -> KnowledgeGraph:
        pipeline = Pipeline.make_pipeline(
            WikipediaRevisionExtractor(
                url=self.config.get("wikipedia", "endpoint"),
                cache=DataCache(self.config.get("data", "extracts_path")),
            ),
            WikitextExtractor(
                cache=DataCache(self.config.get("data", "clean_path")),
            ),
            SpotlightExtractor(
                url=self.config.get("dbpedia", "url"),
                cache=DataCache(self.config.get("data", "surface_term_path")),
            ),
            SubjectExtractor(
                endpoint=self.config.get("dbpedia", "sparql_url"),
                cache=DataCache(self.config.get("data", "subject_path")),
            ),
            WikipediaCategorySampler(
                url=self.config.get("wikipedia", "endpoint"),
                cache=DataCache(self.config.get("data", "category_path")),
            ),
        )

        def reconnect(surface_to_categories, category_to_pagesample):
            to_return = defaultdict(set)
            for surface_term, categories in surface_to_categories:
                for category_i in categories:
                    for category_j, pagesample in category_to_pagesample:
                        if category_j == category_i:
                            to_return[surface_term].update(pagesample)

            return [(subject, list(pages)) for subject, pages in to_return.items()]

        pipeline.run(articles)

        surface_categorysample = reconnect(pipeline.steps[3][1], pipeline.steps[4][1])
        surface_categorysample = [
            (
                "http://dbpedia.org/resource/" + surface_term.replace(" ", "_"),
                page_sample,
            )
            for surface_term, page_sample in surface_categorysample
        ]

        articles_surface = self.to_edgelist(pipeline.steps[2][1])
        resource_category = self.to_edgelist(pipeline.steps[3][1])
        resource_category = [
            ("http://dbpedia.org/resource/" + resource.replace(" ", "_"), category)
            for resource, category in resource_category
        ]
        categorie_subpages = self.to_edgelist(surface_categorysample)

        all_edges = [*articles_surface, *resource_category, *categorie_subpages]
        # all_edges = [*articles_surface, *resource_category]

        graph = KnowledgeGraph.TupleList(all_edges)
        graph = graph.simplify()
        return graph


class ProfileBuilder(object):
    def __init__(self):
        pass

    @staticmethod
    def random_articles(g: KnowledgeGraph, start: str):
        start_v = g.vs.find(start)
        try:
            results = g.random_walk(start_v, steps=40, stuck="error")
        except Exception as e:
            print(e)
            results = []

        # filter out starting vertex
        results = [x for x in results if g.vs[x]["name"] != start_v["name"]]
        results = [g.vs[x]["name"] for x in results]
        results = filter(lambda x: not x.startswith(("Category:")), results)
        return start, list(set(results))

    def __call__(self, g: KnowledgeGraph, articles, nproc=4, **kwargs):
        with Pool(nproc) as p:
            res = p.starmap(self.random_articles, zip(it.repeat(g), articles))

        return res
