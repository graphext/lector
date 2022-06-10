import pandas as pd

from .abc import Reader


class PandasReader(Reader):
    """Use base class detection methods to configure a pandas.read_csv() call."""

    def parse(self, *args, **kwds):
        """Invoke Pandas' parser with inferred CSV format."""
        cfg = {
            "encoding": self.format.encoding,
            "skiprows": self.format.preamble,
            "on_bad_lines": "warn",
            "engine": "python",
        }
        cfg.update(self.format.dialect)

        # Or "\n"? Pandas doesn't allow "\r\n"...
        cfg["lineterminator"] = None

        kwds = {**cfg, **kwds}
        result = pd.read_csv(self.buffer, *args, **kwds)
        return result
