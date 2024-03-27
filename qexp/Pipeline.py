class Pipeline(object):
    def __init__(self, steps):
        self._steps = steps

    @property
    def steps(self):
        return self._steps

    def run(self, xs):
        for idx, (name, step) in enumerate(self._steps):
            xs = step.run(xs)
            self.steps[idx] = (name, xs)

        return xs

    @classmethod
    def make_pipeline(cls, *steps):
        return Pipeline(cls._name_steps(steps))

    @staticmethod
    def _name_steps(steps):
        names = [type(step).__name__.lower() for step in steps]

        return list(zip(names, steps))
