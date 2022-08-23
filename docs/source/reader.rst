CSV Reader
==========

The :doc:`CSV Reader <autoapi/lector/csv/index>` has the simple task of detecting 3
properties of a CSV file:

1. The text encoding (utf-8, latin-1 etc.)
2. A potential preamble (initial lines to skip)
3. The CSV dialect (delimiter etc.)

Lector provides an abstract base class and default implementations for each of
the three detectors (see below).

A reader itself then simply receives instances of these detectors (or the results
of the detection), and configures the parameters of a CSV parser accordingly. The
main CSV parser in lector is pyarrow's `csv.read_csv() <https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html>`_,
as used in the :doc:`ArrowReader <autoapi/lector/csv/arrow/index>`. As an example
for using alternative parsers we also include a :doc:`PandasReader <autoapi/lector/csv/pandas/index>`.
Both implement the abstract :doc:`Reader <autoapi/lector/csv/abc/index>` class.

File encodings
--------------

An encoding detector in lector is any class having a ``detect()`` method that
accepts a binary (bytes) buffer, and returns a string indicating the name of
a `Python codec <https://docs.python.org/3/library/codecs.html>`_, as the
:class:`abstract base class <lector.csv.encodings.EncodingDetector>` requires:

.. code-block:: python

    @dataclass
    class EncodingDetector(ABC):
        """Base class specifying interface for all encoding detetors."""

        @abstractmethod
        def detect(self, buffer: BinaryIO) -> str:
            """Implement me.""


The :class:`default implementation <lector.csv.encodings.Chardet>` uses the
`cchardet <https://github.com/PyYoshi/cChardet>`_ library internally and has the following
interface:

.. code-block:: python

    @dataclass
    class Chardet(EncodingDetector):
        """An encoding detector using cchardet if the default utf-8 generates too many errors."""

        n_bytes: int = int(1e7)  # 10 MB
        """Use this many bytes to detect encoding."""
        error_threshold: float = 0.0
        """A greater proportion of decoding errors than this will be considered a failed encoding."""
        confidence_threshold: float = 0.6
        """Minimum level of confidence to accept an encoding automatically detected by cchardet."""

It reads a maximum of ``n_bytes`` bytes from the received buffer, and then in the following
order:

- Tries to identify an initial byte-order mark (`BOM <https://en.wikipedia.org/wiki/Byte_order_mark>`_)
  indicating the file's codec
- Checks whether assuming ``utf-8`` produces less than ``error_threshold`` decoding errors
  (and returns this codec if true)
- Uses ``cchardet`` to detect the encoding. If cchardet's confidence is greater than the
  ``confidence_threshold``, returns the detected encoding. Otherwise it falls back on the
  ``windows-1250`` codec as the windows/latin-like codec that most acts as a superset of
  special characters amongst related codecs.


Preambles
---------

By "preamble" lector understands initial lines in CSV files to be skipped, e.g. metadata
that should not be interpreted part of the tabular data itself.

It is impossible to always detect arbitrary preambles from the CSV data itself. There are,
however, common patterns amongst preambles written to CSV by certain sources. E.g.
some exporters may separate the metadata from actual data by a line of delimiters only.
Others may write metadata only that does not itself contain the delimiter used otherwise
to separate fields in the tabular part.

Since it is essentially an open-ended exercise to detect arbitrary preambles, lector was
designed to allow easy extension of the patterns to be detected. One simply implements
a new subclass of :class:`PreambleDetector <lector.csv.preambles.PreambleDetector>`, and
uses a decorator to register it with the :class:`preamble registry <lector.csv.preambles.Preambles>`.
Like so:

.. code-block:: python

    @Preambles.register
    @dataclass
    class MyPreamble(PreambleDetector):

        def detect(self, buffer: TextIO) -> int:
            ...

In this case the detector will receive an already decoded *text* buffer, and should
return an integer indicating the number of lines to skip.

:class:`lector.csv.preambles.Brandwatch`, and :class:`lector.csv.preambles.Fieldless`
are two detectors provided out of the box. The former checks for initial lines followed
by a single line of commas only. The second checks for N initial lines containing a single
field only, followed by at least one line containing multiple fields. It then returns N as
the number of rows to skip.

:meth:`lector.csv.preambles.Preambles.detect` is responsible for trying all
implemented detectors in the order they have been registered and returns the first match
(returning N > 0 lines to skip). This may provide too contraining in the long run and
may change in the future so that the order is more easily configurable.

Dialects
--------

The CSV format is not in fact a strict standard, and there are a number of differences
in how CSVs files can be generated. E.g. while the delimiter is usually the comma, it may
also be a semi-colon, the tab or any other arbitrary character. To handle the delimiter
appearing *within* fields, one may choose to quote such fields, or use a special escape
character etc.

A `CSV dialect <https://docs.python.org/3/library/csv.html#dialects-and-formatting-parameters>`_
is a set of parameters describing how to parse a CSV file, i.e. identifying the delimiter,
quote character and so on. In Python's `csv` module, it was decided unfortunately that
to use such dialects one has to pass around subclasses of it, rather than instances. Since
this is somewhat awkward, lector implements it's own :class:`lector.csv.dialects.Dialect`.

Instances of dialects are used as return values by dialect detectors in lector, the abstract
base class of which is simply

.. code-block:: python

    @dataclass
    class DialectDetector(ABC):
        """Base class for all dialect detectors."""

        @abstractmethod
        def detect(self, buffer: TextIO) -> Dialect:
            ...

Lector provides two default implementations. :class:`lector.csv.dialects.PySniffer` uses the
Python standard library's `CSV Sniffer <https://docs.python.org/3/library/csv.html#csv.Sniffer>`_
internally and fixes up the result specifically for more robust *parsing* of CSVs.

Alternatively, if `clevercsv <https://github.com/alan-turing-institute/CleverCSV>`_ has
been installed as an optional dependency, lector wraps it inside the
:class:`lector.csv.dialects.CleverCsv` detector class. It can be used to trade-off speed
against more robust dialect inference.

Readers
-------

Finally, a CSV Reader in lector simply receices an encoding (or encoding detector),
a preamble (or preamble detector) and a dialect (or, wait, a dialect detector). The
abstract base class for readers, :class:`lector.csv.abc.Reader`, is essentially

.. code-block:: python

    class Reader(ABC):
        """Base class for CSV readers."""

        def __init__(
            self,
            fp: FileLike,
            encoding: str | EncodingDetector | None = None,
            dialect: dict | DialectDetector | None = None,
            preamble: int | PreambleRegistry | None = None,
            log: bool = True,
        ) -> None:
            self.fp = fp
            self.encoding = encoding or encodings.Chardet()
            self.dialect = dialect or dialects.PySniffer()
            self.preamble = preamble or Preambles
            self.log = log

    def read(self, *args, **kwds) -> Any:
        try:
            self.analyze()
            result = self.parse(*args, **kwds)
            self.buffer.close()
            return result
        except Exception:
            raise

    @abstractmethod
    def parse(self, *args, **kwds) -> Any:
        """Parse the file pointer or text buffer. Args are forwarded to read()."""
        ...

The base class uses the provided detectors to infer (if necessary) all the information
required to call a CSV parser. It wraps all inferred information in a
:class:`lector.csv.abc.Format` object, which Reader subclasses can then translate
to a specific parser's own parameters. E.g., the only thing the :class:`lector.csv.arrow.ArrowReader`
does, is translate a CSV Format, to arrow's own ``csv.ReadOptions``, ``csv.ParseOptions``
and ``csv.ConvertOptions`` objects.

If no parameters (other than a file pointer) are passed, a reader uses the default
implementations of all detectors, which means that if no customization is needed,
reading almost any CSV becomes simply:

.. code-block:: python

    from lector import ArrowReader

    tbl = ArrowReader("/path/to/file.csv").read()
