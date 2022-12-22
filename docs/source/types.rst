Types
=====

Introduction
------------

Lector implements its own column type inference. It can be used by parsing a CSV file
with ``string`` types only (preserving the original fields without modification),
and then auto-casting all columns to the most appropriate and efficient data type:

.. code-block:: python

    import io
    from lector import ArrowReader, Autocast
    from lector.log import schema_view

    csv = """id,genre,metric,count,content,website,tags
    1234982348728374,a,0.1,1,, http://www.graphext.com,"[a,b,c]"
    ,b,0.12,,"Natural language text is different from categorical data.", https://www.twitter.com,[d]
    18446744073709551615,a,3.14,3,"The Project · Gutenberg » EBook « of Die Fürstin.",http://www.google.com,"['e', 'f']"
    """

    tbl = ArrowReader(io.StringIO(csv)).read(types="string")
    tbl = Autocast().cast(tbl)
    schema_view(tbl.schema)

Printing the table schema this way will produce the following output:

.. code-block::

    Schema
     ─────────────────────────────────────────────────────────
     Column    Type           Meta
     ─────────────────────────────────────────────────────────
     id        uint64         {'semantic': 'number[UInt64]'}
     genre     dict<string>   {'semantic': 'category'}
     metric    double         {'semantic': 'number[double]'}
     count     uint8          {'semantic': 'number[UInt8]'}
     content   string         {'semantic': 'text'}
     website   dict<string>   {'semantic': 'url'}
     tags      list<string>   {'semantic': 'list[category]'}
     ─────────────────────────────────────────────────────────

The schema view contains for each column in the table the arrow type it has been
cast to, as well as some metadata about the *semantic* type lector has inferred.
By semantic type we mean the kind of content the column contains, which may be
different from (more specific than) the "physical" (arrow) type used to store it.

For example, the "website" column has been cast to arrow using a dictionary type with
string values (``dict<string>``). However, lector has in fact inferred that the column
contains URLs, and simply selected the dictionary type as the best storage type for URLs.
Equally, the "content" column has been inferred to contain natural language text, and in
this case arrow's ``string`` type is used for storage. Also note that lector handles
types that have no equivalent in `pandas`. The "tags" column contains lists of strings, for
example, which lector has automatically parsed and cast to arrow's ``list<string>``
type.

For numeric columns lector has automatically identified the most efficient (least
memory-hungry) data types. The semantic metadata here is used to indicate pandas'
corresponding (potentially nullable extension) ``dtype``.

Using lector's ``to_pandas()`` function we can convert the arrow table to a pandas DataFrame
ensuring that all data is converted correctly, even when there is no corresponding
pandas type:

.. code-block:: python

    from lector.utils import to_pandas

    df = to_pandas(tbl)
    print(df)
    print(df.dtypes)

produces

.. code-block::

                        id genre  metric  count  \
    0      1234982348728374     a    0.10      1
    1                  <NA>     b    0.12   <NA>
    2  18446744073709551615     a    3.14      3

                                                content                  website  \
    0                                               <NA>  http://www.graphext.com
    1  Natural language text is different from catego...  https://www.twitter.com
    2  The Project · Gutenberg » EBook « of Die Fürstin.    http://www.google.com

            tags
    0  [a, b, c]
    1        [d]
    2     [e, f]


    id           UInt64
    genre      category
    metric      float64
    count         UInt8
    content      string
    website    category
    tags         object
    dtype: object

Note that arrow's ``tbl.to_pandas()`` would have converted integer columns with
missing data to the float type, which is not save and may introduce erroneous data
(because of insufficient float precision when representing large integers). Lector
uses extension dtypes where necessary. Also note how all tag lists have been parsed
correctly, despite having various different representations in the CSV data (use of
quotes etc.). In pandas, the lists are representated by a column of numpy arrays.

Array Converters
----------------

``Converters`` in Lector are responsible for inferring the semantic type of a column
(i.e. an arrow Array or ChunkedArray), identifying the corresponding storage type,
and potentially generating some useful metadata.

Lector implements one subclass of :class:`lector.types.abc.Converter` for each semantic
type. At the moment there are:

- :class:`lector.types.numbers.Number` (``number``)
- :class:`lector.types.lists.List` (``list[number]`` or ``list[category]``)
- :class:`lector.types.strings.Text` (``text``)
- :class:`lector.types.strings.Category` (``category``)
- :class:`lector.types.strings.Url` (``url``)
- :class:`lector.types.timestamps.Timestamp` (``date``)

Their interface is simple. Minimally they have to accept a ``threshold`` as
parameter, and must implement a ``convert()`` method:

.. code-block:: python

    @dataclass
    class Converter(ABC):

        threshold: float = 1.0

        @abstractmethod
        def convert(self, arr: Array) -> Conversion | None:
            ...

    @dataclass
    class Conversion:

        result: Array
        meta: dict = field(default_factory=dict)

A specific converter (e.g. ``Number``) returns ``None`` if the passed
array data is not compatible with the type (e.g. the values are not numeric).
Otherwise it will return a ``Conversion`` object containing the correctly
cast array and potential metadata.

The converters can be used in two ways for casting a table of raw data: using
:class:`lector.types.cast.Autocast` to infer the best type automatically, or
using :class:`lector.types.cast.Cast` to  specify the (semantic) type for each
column explicitly (see below for more information).

In both cases, exactly *how* types are used for inference and casting can be
configured by

- limiting or extending the list of allowed semantic types/converters
- configuring each converter via parameters

The single common parameter for all converters is the ``threshold``. This
is used to identify the proportion of values in an array that have to be
valid according to the given type for it to return a cast result. I.e.
a converter (e.g. ``Number``) should return ``None`` if the proportion
of valid (e.g. numeric-like) values is less than ``threshold``.

Automatic Table Cast
--------------------

For each column, the :class:`lector.types.cast.Autocast` simply tries each semantic
type (i.e. ``Converter``) in an ordered list. The first ``Converter`` returning a
``Conversion`` (rather than ``None``) is used to cast the column. If no list of
converters is specified explicitly (as in the first example above), a default
order of all implemented converters with default parameters is used
(:data:`lector.types.cast.DEFAULT_CONVERTERS`).

As mentioned above, the ``Autocast`` can be configured by passing an ordered list
of allowed converters, and by configuring the parameters of individual converters.
If that is not enough, and a more complicated cast strategy is required, one can
also implement a new subclass of :class:`lector.types.cast.CastStratregy`. The base
class takes care of iteration over columns and some other minor boilerplate so
that subclasses only have to implement the conversion of a single Array.

Explicit Table Cast
-------------------

:class:`lector.types.cast.Cast` is even simpler. Given a mapping of column names to
converters, it applies a specific converter to a specific column. If the conversion
is successful that column is cast, otherwise left as is. For example, given the CSV
data above:

.. code-block:: python

    from lector import Cast
    from lector.types import Category, Number

    types = {
        "id": Number(),
        "genre": Category(max_cardinality=None)
    }

    tbl = ArrowReader(io.StringIO(csv)).read(types="string")
    tbl = Cast(types).cast(tbl)
    schema_view(tbl.schema)

produces

.. code-block::

    Schema
     ─────────────────────────────────────────────────────────
     Column    Type           Meta
     ─────────────────────────────────────────────────────────
     id        uint64         {'semantic': 'number[UInt64]'}
     genre     dict<string>   {'semantic': 'category'}
     metric    string
     count     string
     content   string
     website   string
     tags      string
     ─────────────────────────────────────────────────────────

I.e., only the two specified columns have been converted using the configured
types.
