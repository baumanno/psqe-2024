import logging
import typing
from itertools import islice, repeat
from urllib.parse import unquote

import requests

from qexp import types
from qexp.cache import DataCache


class BaseWikipediaExtractor(object):
    def __init__(self, url: str, cache: DataCache):
        self.results = []
        self.url = url
        self.session = requests.Session()
        self.logger = logging.getLogger("main")
        self.cache = cache

        self.session.headers.update(
            {
                "accept": "application/json",
                "user-agent": (
                    "libcurl/7.81.0 oliver.baumann@uni-bayreuth.de" " requests/2.28.2"
                ),
                "Accept-Encoding": "gzip",
            }
        )

    @staticmethod
    def _chunk(xs: typing.Iterable, size: int) -> typing.Iterator:
        it = iter(xs)
        return iter(lambda: tuple(islice(it, size)), ())

    def _query(self, query_items: typing.Dict[str, str], nocontinue=False):
        self.logger.info("Querying {}".format(",".join(query_items.values())))
        last_continue: typing.Dict[str, str] = {}
        while True:
            self.session.params.update(last_continue)  # pyright: ignore
            self.session.params.update(query_items)  # pyright: ignore
            res = self.session.get(self.url).json()

            if "error" in res:
                raise SystemError(res["error"])
            if "warnings" in res:
                self.logger.warning(res["warnings"])
            if "query" in res:
                yield res["query"]
            if "continue" not in res:
                break
            if nocontinue:
                break
            last_continue = res["continue"]


class WikipediaCategorySampler(BaseWikipediaExtractor):
    def __init__(self, url: str, cache: DataCache):
        super().__init__(url, cache)
        self.session.params = {
            "action": "query",
            "generator": "categorymembers",
            "gcmnamespace": "0",
            "prop": "info",
            "format": "json",
            "formatversion": "2",
        }

    def run(
        self, things: typing.List[typing.Tuple[str, str]]
    ) -> typing.List[typing.Tuple[str, str]]:
        to_fetch = []
        to_return: typing.List[typing.Tuple[str, str]] = []
        things = [
            (concept, unquote(category))
            for concept, categories in things
            for category in categories
        ]

        for concept, category in things:
            sub_pages = self.cache.get(category)
            if sub_pages is None:
                to_fetch.append(category)
                continue
            else:
                to_return.append((category, sub_pages.split(";")))

        self.logger.info(
            "Extracting cat-members for {} categories".format(len(to_fetch))
        )
        for category in to_fetch:
            # check cache again, it may have been updated already
            sub_pages = self.cache.get(category)
            if sub_pages is not None:
                to_return.extend(zip(repeat(category), sub_pages.split(";")))
                continue

            # nocontinue=True, because we don't need the entire cat-tree, the
            # first page is sufficient
            for res in self._query(
                {"gcmtitle": "{}".format(category)}, nocontinue=True
            ):
                pages = []
                for page in res["pages"]:
                    title = page["title"].strip().replace(" ", "_")
                    pageid = str(page["pageid"]).strip()

                    if not title.startswith(("File:", "Template:", "Category:")):
                        pages.append(pageid)

                self.cache.set(category, ";".join(pages))
                to_return.extend(zip(repeat(category), pages))
                break
            else:
                self.cache.set(category, "")
                to_return.extend([(category, "")])

        return to_return


class WikipediaRevisionExtractor(BaseWikipediaExtractor):
    def __init__(self, url: str, cache: DataCache):
        super().__init__(url, cache)
        self.session.params = {
            "action": "query",
            "prop": "revisions",
            "rvprop": "content",
            "rvslots": "*",
            "format": "json",
            "formatversion": "2",
        }

    def run(
        self, article_ids: typing.Iterable[str]
    ) -> typing.List[typing.Tuple[str, str]]:
        to_fetch = []
        to_return: typing.List[typing.Tuple[str, str]] = []
        for article_id in article_ids:
            if len(article_id) == 0:
                continue
            revision = self.cache.get(article_id)
            if revision is None:
                to_fetch.append(article_id)
                continue
            else:
                to_return.append((article_id, revision))

        for article_id in self._chunk(to_fetch, 5):
            # article_id = list(filter(lambda x: len(x)>1, article_id))
            self.logger.info("Querying {} revisions".format(len(article_id)))
            for res in self._query({"pageids": "|".join(article_id)}):
                for page in res["pages"]:
                    if "missing" in page and page["missing"]:
                        continue
                    pageid = str(page["pageid"]).strip()
                    revision = page["revisions"][0]["slots"]["main"]["content"]
                    self.cache.set(
                        pageid,
                        revision,
                    )

                    to_return.append((pageid, revision))

        return to_return


class PageLinkExtractor(BaseWikipediaExtractor):
    def __init__(
        self,
        url: str,
        cache: DataCache,
        exclude_list: typing.Iterable[types.PageID],
        no_continue: bool,
    ):
        super().__init__(url, cache)
        self.no_continue = no_continue
        self.exclude_list = exclude_list
        self.session.params = {
            "action": "query",
            "generator": "links",
            "gplnamespace": "0",
            "gpllimit": "100",
            "prop": "info",
            "format": "json",
            "formatversion": "2",
        }

    def run(
        self, page_titles: typing.Iterable[types.PageTitle]
    ) -> typing.List[types.PipelineListResult]:
        to_return = []
        to_fetch = []
        for page_title in page_titles:

            links = self.cache.get(page_title)
            if links is None:
                to_fetch.append(page_title)
                continue
            else:
                links = links.split(",")
                to_return.append((page_title, links))

        for page_title in to_fetch:
            self.logger.info(f"Querying links for {page_title}")

            for res in self._query(
                {"titles": page_title.replace(" ", "_")}, nocontinue=self.no_continue
            ):
                link_ids = []
                for page in res["pages"]:
                    if (
                        "missing" in page and page["missing"] is True
                    ) or not "pageid" in page:
                        continue
                    if str(page["pageid"]) in self.exclude_list:
                        continue

                    link_ids.append(str(page["pageid"]))

                self.cache.set(page_title, ",".join(link_ids))

                to_return.append((page_title, link_ids))

        return to_return
