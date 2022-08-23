Lector
======

`Lector <https://github.com/graphext/lector>`_ aims to be a fast reader for potentially
messy CSV files with configurable column type inference. It combines automatic detection
of :ref:`reader:File encodings` , :ref:`CSV dialects <reader:Dialects>` (separator,
escaping etc.) and :ref:`reader:preambles` (initial lines
containing metadata or junk unrelated to the actual tabular data). Its goal is to
just-read-the-effing-CSV file without manual configuration in most cases. Each of the
detection components is configurable and can be swapped out easily with custom implementations.

Also, since both `pandas <https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html>`_
and  Apache's `arrow <https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html>`_
will destructively cast columns to the wrong type in some cases (e.g. large ID-like integer
strings to floats), it provides an alternative and customisable column type :doc:`inference and
casting <types>` mechanism.

Under the hood it uses pyarrow's CSV parser for reading, and its compute functions for optional
type inference.

Lector is used at `Graphext <https://www.graphext.com>`_ behind the scenes whenever a user
uploads a new dataset, and so implicitly has been validated across 1000s of different CSV
files from all kinds of sources.

Note, however, that this is Graphext's first foray into open-sourcing our code and still
*work-in-progress*. So at least initially we won't provide any guarantees as to support
of this library.

For a quick illustration of how to use ``lector``, see the the :doc:`quickstart guide <quick>`.


.. toctree::
   :hidden:

   Home  <self>
   Quickstart <quick>
   CSV Reader <reader>
   Types <types>
   API <autoapi/lector/index>
