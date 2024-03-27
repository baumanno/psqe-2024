import csv
import pathlib


class SearchSpec(object):
    def __init__(self, query: str, article_id: str, parent_id: str, do_expansion: bool):
        self.parent_id = parent_id
        self.do_expansion = do_expansion
        self.article_id = article_id
        self.query = query


class SearchSpecsBuilder(object):
    def __init__(self, file: pathlib.Path):
        self.file = file
        self.specs = list(self._build())
        self._index = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            result = self.specs[self._index]
        except IndexError:
            raise StopIteration

        self._index += 1
        return result

    def _build(self):
        with open(self.file, "r", newline="") as f:
            reader = csv.reader(f, delimiter=",")
            # skip the header
            next(reader)
            for line in reader:
                yield SearchSpec(
                    query=line[3].lower().strip(),
                    article_id=line[0],
                    parent_id=line[2],
                    do_expansion=line[4] == "True",
                )


class Result(object):
    HEADER = [
        "article_id",
        "parent_id",
        "doc_id",
        "score",
        "query",
        "with_augment",
        "was_augmented",
    ]

    def __init__(
        self,
        article_id: str,
        parent_id: str,
        doc_id: str,
        score: float,
        query: str,
        with_augment: bool,
        was_augmented: bool,
    ):
        self.article_id = article_id
        self.parent_id = parent_id
        self.doc_id = doc_id
        self.score = score
        self.query = query
        self.with_augment = with_augment
        self.was_augmented = was_augmented

    def __iter__(self):
        return iter([getattr(self, k) for k in self.HEADER])
