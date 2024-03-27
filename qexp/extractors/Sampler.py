import random
import typing

from qexp import types


class Sampler(object):
    def __init__(self, sample_size: int):
        self.sample_size = sample_size

    def run(self, collection: typing.Sequence[types.PipelineListResult]):
        to_return = []
        for k, xs in collection:
            random.seed(1234)
            to_return.append((k, random.sample(xs, k=min(len(xs), self.sample_size))))

        return to_return
