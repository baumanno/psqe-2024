import logging
import typing

import requests

from qexp.cache import DataCache


class SpotlightExtractor(object):
    def __init__(
        self,
        url: str,
        cache: DataCache,
    ):
        self.url = url
        self.annotate_endpoint = url + "/annotate"
        self.logger = logging.getLogger("main")
        self.cache = cache

    def run(self, items: typing.Iterable) -> typing.List[typing.Tuple[str, str]]:
        to_return = []
        for article_id, clean_text in items:
            annotations = self.cache.get(article_id)
            if annotations is not None:
                to_return.append((article_id, annotations.split(";")))
                continue

            response = requests.get(
                self.annotate_endpoint,
                params={"text": clean_text.encode("utf-8")[:7100], "confidence": "0.6"},
                headers={"Accept": "application/json"},
            )
            try:
                response = response.json()
            except:
                raise SystemError(response.text)

            # Spotlight returns a "Resources"-list, containing the identified
            # annotation-objects.
            try:
                surface_terms = []
                for resource in response["Resources"]:
                    surface_terms.append(resource["@URI"])

                surface_terms = list(set(surface_terms))
                self.cache.set(article_id, ";".join(surface_terms))
                to_return.append((article_id, surface_terms))
            except Exception as e:
                raise e

        return to_return
