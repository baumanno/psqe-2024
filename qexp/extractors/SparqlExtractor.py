import logging
import random
import time
import typing

import redis
from SPARQLWrapper import JSON, SPARQLWrapper

from qexp import DataCache


class BaseSparqlExtractor(object):
    def __init__(self, endpoint: str, cache: DataCache):
        self.sparql = SPARQLWrapper(endpoint)
        self.sparql.setReturnFormat(JSON)
        self.logger = logging.getLogger("main")
        self.cache = cache


class WikiPageIdExtractor(BaseSparqlExtractor):
    def __init__(self, endpoint: str, cache: DataCache = None):
        super().__init__(endpoint, cache)
        self.redis = redis.Redis(host="localhost", port=6379, decode_responses=True)

    def run(
        self, items: typing.Iterable[typing.Tuple[str, typing.List[str]]]
    ) -> typing.List[typing.Tuple[str, typing.List[str]]]:
        query = """\
SELECT DISTINCT  ?id
FROM <http://dbpedia.org>
WHERE {{
    <{}> <http://dbpedia.org/ontology/wikiPageID> ?id .
}}
LIMIT  1
        """
        to_return = []
        for start_id, alters in items:
            new_alters = []
            for alter in alters:
                if self.redis.get(alter):
                    self.logger.info("Found Redis key")
                    new_alters.append(self.redis.get(alter))
                    continue

                if not alter.startswith("http://"):
                    self.logger.debug("skipping")
                    new_alters.append(alter)
                    continue

                formatted_query = query.format(alter)
                self.sparql.setQuery(formatted_query)

                self.logger.info("SPARQL ID query for {}".format(alter))

                try:
                    time.sleep(random.randint(200, 1000) / 1000)
                    ret = self.sparql.queryAndConvert()
                    self.redis.set(alter, ret["results"]["bindings"][0]["id"]["value"])
                    new_alters.append(ret["results"]["bindings"][0]["id"]["value"])

                except Exception as e:
                    self.logger.error(e)
            to_return.append((start_id, new_alters))

        return to_return


class SubjectExtractor(BaseSparqlExtractor):
    def __init__(self, endpoint: str, cache: DataCache):
        super().__init__(endpoint, cache)
        self.blocklist = [
            "Alcoholic drink",
            "Federal Insurance Contributions Act tax",
            "Victor Perez",
            "Kino International (company)",
            "The Fabulous Freebirds",
            "Taps",
            "The Blade Runners",
            "Divine Intervention (film)",
            "Google mobile services",
            "Anil Sharma",
            "Jayaprada",
            "Bollywood",
            "Anil Sharma",
            "Nathan Petrelli",
            "13 Minutes",
            "13 Minutes",
            "The Units",
            "Odessa",
        ]

    def run(self, items: typing.Iterable[str]) -> typing.List[typing.Tuple[str, str]]:
        query = """\
SELECT DISTINCT  ?subjects
FROM <http://dbpedia.org>
WHERE {{
    ?concept <http://www.w3.org/2000/01/rdf-schema#label> "{}"@en ;
    <http://purl.org/dc/terms/subject> ?subjects .
}}
LIMIT  100
"""
        to_return = []
        for article_id, labels in items:
            for label in labels:
                label = (
                    label.removeprefix("http://dbpedia.org/resource/")
                    .strip()
                    .replace("_", " ")
                )
                if label in self.blocklist:
                    continue
                subjects = self.cache.get(label)
                if subjects is not None:
                    to_return.append((label, subjects.split(";")))
                    continue

                formatted_query = query.format(label)
                self.sparql.setQuery(formatted_query)

                self.logger.info("SPARQL query for {}".format(label))

                try:
                    time.sleep(random.randint(200, 5000) / 1000)
                    ret = self.sparql.queryAndConvert()
                    subjects = []
                    for res in ret["results"]["bindings"]:
                        self.logger.info("found {}".format(res))
                        subjects.append(
                            res["subjects"]["value"].removeprefix(
                                "http://dbpedia.org/resource/"
                            )
                        )
                        subjects = list(set(subjects))
                        to_return.append((label, subjects))
                        self.cache.set(label, ";".join(subjects))
                except Exception as e:
                    self.logger.error(e)

        return to_return
