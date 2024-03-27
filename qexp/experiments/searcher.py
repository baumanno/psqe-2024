import logging
import typing

from qexp.expand import ModelRegistry, Query
from qexp.experiments.types import Result, SearchSpec
from qexp.search import Search


class Searcher(object):
    def __init__(
        self,
        specs: typing.Iterable[SearchSpec],
        search: Search,
        registry: ModelRegistry,
    ):
        self.specs = specs
        self.search = search
        self.registry = registry

    def run(self):
        results = []
        for spec in self.specs:
            query = spec.query

            q = Query(query, spec.article_id)

            if spec.do_expansion:
                model = self.registry.get_model(spec.article_id)

                q = q.expand(model)

                logging.getLogger("main").info(f"query {q.query}")

            search_results = self.search.search(q.query)

            for s in search_results[:100]:
                results.append(
                    Result(
                        article_id=spec.article_id,
                        parent_id=spec.parent_id,
                        doc_id=s["ref"],
                        score=s["score"],
                        query=q.query,
                        with_augment=spec.do_expansion,
                        was_augmented=q.expanded,
                    )
                )
        return results
