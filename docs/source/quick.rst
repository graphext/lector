Quickstart
==========


Installation
------------

While this library is not available yet on pypi, you can easily install it from Github with

.. code-block:: bash

    pip install git+https://github.com/graphext/lector

The project depends on ``cchardet`` for encoding detection, ``clevercsv`` for advanced
dialect detection, ``pyarrow`` for CSV parsing and type inference/conversion, as well as
``rich`` and ``typer`` for pretty output and the command-line interface.

Quickstart
----------

The below examples illustrate lector's default behaviour when reading CSV files. For
customization options, check the :doc:`reader` and :doc:`types` sections as well as the
:doc:`API reference <autoapi/lector/index>`.

Let's assume we receive the following CSV file, containing some initial metadata, using
the semicolon as separator, having some missing fields, and being encoded in Latin-1:

.. code-block:: python

    csv = """
    Some metadata
    Some more metadata
    id;category;metric;count;text
    1234982348728374;a;0.1;1;
    ;b;0.12;;"Natural language text is different from categorical data."
    18446744073709551615;a;3.14;3;"The Project · Gutenberg » EBook « of Die Fürstin."
    """.encode("ISO-8859-1")

The recommended way to use lector for reading this CSV would be

.. code-block:: python

    from lector import ArrowReader

    tbl = ArrowReader(io.BytesIO(csv)).read(types="string")

which produces something like the following output::

    'Fieldless' matches CSV buffer: detected 3 rows to skip.

    ─────────── CSV Format ────────────
    {
        'encoding': 'ISO-8859-1',
        'preamble': 3,
        'dialect': Dialect(
            delimiter=';',
            quote_char='"',
            escape_char=None,
            double_quote=True,
            skip_initial_space=False,
            line_terminator='\r\n',
            quoting=0
        )
    }
    ───────────────────────────────────

The log provides some feedback about proporties of the CSV that lector has detected
automatically, namely:

- It has found a *preamble* pattern named ``Fieldless`` that matches the beginning of the
  CSV file and indicates that the first 3 rows should be skipped (lector has an extensible
  list of such patterns which are tried in order until a match is found)
- It has detected the *encoding* correctly as ``ISO-8859-1`` (this cannot be guaranteed in all
  cases, but the CSV will be read always with a fallback encoding, usually ``utf-8``, and
  characters that cannot be decoded will be represented by �)
- It has correctly detected the CSV *dialect* (the delimiter used etc.)
- The encoding, preamble and dialect together are stored in a ``Format`` object, which holds
  all the necessary parameters to parse the CSV file correctly with pandas or arrow

Using the detected CSV format, the data is parsed (using pyarrow's ``csv.read_csv()`` under
the hood). Note we have indicated to arrow to parse all columns using the ``string`` type,
effectively turning *off* its internal type inference. We apply lector's type inference with

.. code-block:: python

    from lector import Autocast

    tbl = Autocast().cast(tbl)
    print(tbl.schema)

We see this results in the most appropriate type for each column:

.. code-block::

    pyarrow.Table
    id: uint64
    category: dictionary<values=string, indices=int32, ordered=0>
    metric: double
    count: uint8
    text: string

Notice that:

- An unsigned int (``uint64``) was necessary to correctly represent all values in the id
  column. Had values been even larger than the maximum of the ``uint64`` type, the values
  would have been converted to a categorical type (strings), rather than floats
- The category column was automatically converted to the memory-efficient ``dictionary``
  (categorical) type
- The count column uses the smallest integer type necessary (``uint8``, unsigned since all
  values are positive)
- The text column, containing natural language text, has *not* been converted to a categorical
  type, but kept as simple ``string`` values (it is unlikely to benefit from dictionary-encoding)

We could have relied on arrow's internal type inference instead with the following single-liner:

.. code-block:: python

    typed_table = ArrowReader(io.BytesIO(csv)).read()

but this would result in less memory-efficient and even erroneous data types (see the
pandas and pure arrow comparisons below).

Finally, if you need the CSV table in pandas, lector provides a little helper for correct
conversion (again, pure arrow's to_pandas(...) isn't smart or flexible enough to use pandas
extension dtypes for correct conversion):

.. code-block:: python

    from lector.utils import to_pandas

    df = to_pandas(tbl)
    print(df)
    print(df.types)

Which outputs::

                        id category  metric  count  \
    0      1234982348728374        a    0.10      1
    1                  <NA>        b    0.12   <NA>
    2  18446744073709551615        a    3.14      3

                                                    text
    0                                               <NA>
    1  Natural language text is different from catego...
    2  The Project · Gutenberg » EBook « of Die Fürstin.

    id            UInt64
    category    category
    metric       float64
    count          UInt8
    text          string
    dtype: object

Note how nullable pandas extension dtypes are used to preserve correct integer values, where pure arrow would have used the unsafe float type instead.

Compared with pandas
--------------------

Trying to read CSV files like the above using ``pandas.read_csv(...)`` and default arguments
only will fail. To find the correct arguments, you'll have to open the CSV in a text editor
and manually identify the separator and the initial lines to skip, and then try different
encodings until you find one that seems to decode all characters correctly. But even if you
then manage to read the CSV, the result may not be what you expected:

.. code-block:: python

    csv = """
    Some metadata
    Some more metadata
    id;category;metric;count;text
    1234982348728374;a;0.1;1;"This is a text."
    ;b;0.12;;"Natural language text is different from categorical data."
    9007199254740993;a;3.14;3;"The Project · Gutenberg » EBook « of Die Fürstin."
    """.encode("ISO-8859-1")

    df = pd.read_csv(
        io.BytesIO(csv),
        encoding="ISO-8859-1",
        skiprows=3,
        sep=";",
        index_col=False
    )
    print(df)
    print(df.dtypes)

results in::

                id  category   metric   count  \
    0  1.234982e+15         a     0.10     1.0
    1           NaN         b     0.12     NaN
    2  9.007199e+15         a     3.14     3.0

                                                    text
    0                                    This is a text.
    1  Natural language text is different from catego...
    2  The Project · Gutenberg » EBook « of Die Fürstin.


    id          float64
    category    object
    metric      float64
    count       float64
    text        object


The ``category`` and ``text`` columns have been imported with the ``object`` dtype,
which is not particularly useful, but not necessarily a problem either.

Note, however, that numeric-like columns with missing data have been cast to the ``float``
type. This may seem merely a nuisance in the case of the ``count`` column, which could easily
be cast to a (nullable) integer type. It is, however, a big problem for the ``id`` column,
since not all integers can be represented exactly by a 64 bit floating type:

.. code-block::

    >>> print(df.id.iloc[2])
    9007199254740992.0

which is not the value ``"9007199254740993"`` contained in our CSV file! We cannot cast
this column to the correct type anymore either (e.g. ``int64`` or ``string``), because
the original value is lost. It is also a sneaky problem, because you may not realize
you've got wrong IDs, and may produce totally wrong analyses if you use them down the
line for joins etc. The only way to import CSV files like this correctly is to inspect
essentially all columns and all rows manually in a text editor, choose the best data type
manually, and then provide these types via pandas ``dtype`` argument. This may be feasible
if you work with CSVs only sporadically, but quickly becomes cumbersome otherwise.


Compared with arrow
-------------------

The arrow CSV reader unfotunately faces exactly the same limitations as pandas:

.. code-block:: python

    import pyarrow as pa
    import pyarrow.csv


    csv = """
    Some metadata
    Some more metadata
    id;category;metric;count;text
    1234982348728374;a;0.1;1;
    ;b;0.12;;"Natural language text is different from categorical data."
    18446744073709551615;a;3.14;3;"The Project · Gutenberg » EBook « of Die Fürstin."
    """.encode("ISO-8859-1")

    tbl = pa.csv.read_csv(
        io.BytesIO(csv),
        read_options=pa.csv.ReadOptions(encoding="ISO-8859-1", skip_rows=3),
        parse_options=pa.csv.ParseOptions(delimiter=";"),
        convert_options=pa.csv.ConvertOptions(strings_can_be_null=True)
    )

    print(tbl)
    int(tbl.column("id")[2].as_py())

It needs the same level of human inspection to identify the correct arguments to read the CSV,
and destructively casts IDs to floats (but at least uses a more efficient string type where
applicable, in contrast to pandas object dtype)::

    pyarrow.Table
    id: double
    category: string
    metric: double
    count: int64
    text: string
    ----
    id: [[1.234982348728374e+15,null,1.8446744073709552e+19]]
    category: [["a","b","a"]]
    metric: [[0.1,0.12,3.14]]
    count: [[1,null,3]]
    text: [[null,"Natural language text is different from categorical data.","The Project · Gutenberg » EBook « of Die Fürstin."]]

    18446744073709551616

Again, the only way to ensure correctness of the parsed CSV is to not use arrow's built-in
type inference, but provide the desired type for each column manually.
