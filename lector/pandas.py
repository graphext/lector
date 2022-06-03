import pandas as pd

from .abc import Reader


class PandasReader(Reader):
    """Use base class detection methods to configure a pandas.read_csv() call."""

    def configure(self):
        cfg = {
            "encoding": self.encoding,
            "skiprows": self.preamble,
            "on_bad_lines": "warn",
            "engine": "python",
        }
        cfg.update(self.dialect)
        # Or "\n"? Pandas doesn't allow "\r\n"...
        cfg["lineterminator"] = None
        self.config = cfg

    def parse(self, *args, **kwds):
        kwds = {**self.config, **kwds}
        result = pd.read_csv(self.buffer, *args, **kwds)
        return result
