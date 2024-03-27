import logging
import re
import typing

import mwparserfromhell

from qexp.cache import DataCache


class WikitextExtractor(object):
    # Kudos to earwigbot, whence this code originated
    # https://github.com/earwig/earwigbot/blob/develop/earwigbot/wiki/copyvios/parsers.py

    TEMPLATE_MERGE_THRESHOLD = 25

    def __init__(
        self,
        cache: DataCache,
    ):
        self.filename = ""
        self.logger = logging.getLogger("main")
        self.cache = cache

    def run(self, items) -> typing.List[typing.Tuple[str, str]]:
        cleaned_revisions = []
        for article_id, revision_with_markup in items:
            # Return early if the cache already contains the cleaned extract
            clean_text = self.cache.get(article_id)
            if clean_text is not None:
                cleaned_revisions.append((article_id, clean_text))
            else:
                clean_text = self.strip(revision_with_markup)
                self.cache.set(article_id, clean_text)
                cleaned_revisions.append((article_id, clean_text))

        return cleaned_revisions

    def strip(self, text: str):
        """Clean the page's raw text by removing templates and formatting.
        Return the page's text with all HTML and wikicode formatting removed,
        including templates, tables, and references. It retains punctuation
        (spacing, paragraphs, periods, commas, (semi)-colons, parentheses,
        quotes), original capitalization, and so forth. HTML entities are
        replaced by their unicode equivalents.
        The actual stripping is handled by :py:mod:`mwparserfromhell`.
        """

        def remove(code, node):
            """Remove a node from a code object, ignoring ValueError.
            Sometimes we will remove a node that contains another node we wish
            to remove, and we fail when we try to remove the inner one. Easiest
            solution is to just ignore the exception.
            """
            try:
                code.remove(node)
            except ValueError:
                pass

        wikicode = mwparserfromhell.parse(text)

        # Preemtively strip some links mwparser doesn't know about:
        bad_prefixes = ("file:", "image:", "category:")
        for link in wikicode.filter_wikilinks():
            if link.title.strip().lower().startswith(bad_prefixes):
                remove(wikicode, link)

        for tpl in wikicode.filter_templates():
            remove(wikicode, tpl)

        # Also strip references:
        for tag in wikicode.filter_tags(matches=lambda tag: tag.tag == "ref"):
            remove(wikicode, tag)

        # Also strip tables:
        for tag in wikicode.filter_tags(matches=lambda tag: tag.tag == "table"):
            remove(wikicode, tag)

        # Merge in template contents when the values are long:
        self._merge_templates(wikicode)

        clean = wikicode.strip_code(normalize=True, collapse=True)
        clean = re.sub(r"\n\n+", "\n", clean).strip()

        return clean

    def _merge_templates(self, code):
        """Merge template contents in to wikicode when the values are long."""
        for template in code.filter_templates(recursive=code.RECURSE_OTHERS):
            chunks = []
            for param in template.params:
                if len(param.value) >= self.TEMPLATE_MERGE_THRESHOLD:
                    self._merge_templates(param.value)
                    chunks.append(param.value)
            if chunks:
                subst = " ".join(map(str, chunks))
                code.replace(template, " " + subst + " ")
            else:
                code.remove(template)
