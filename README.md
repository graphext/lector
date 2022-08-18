# Lector

[Lector](https://github.com/graphext/lector) aims to be a fast reader for potentially messy CSV files with configurable column type inference. It combines automatic detection of file encodings, CSV dialects (separator, escaping etc.) and preambles (initial lines containing metadata or junk unrelated to the actual tabular data). Its goal is to just-read-the-effing-CSV file without manual configuration in most cases. Each of the detection components is configurable and can be swapped out easily with custom implementations.

Also, since both [pandas](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html) and Apache [arrow](https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html) will destructively cast columns to the wrong type in some cases (e.g. large ID-like integer strings to floats), it provides an alternative and customisable inference and casting mechanism.

Under the hood it uses pyarrow's CSV parser for reading, and its compute functions for optional type inference.

Lector is used at [Graphext](https://www.graphext.com) behind the scenes whenever a user uploads a new dataset, and so implicitly has been validated across 1000s of different CSV files from all kinds of sources.

Note, however, that this is Graphext's first foray into open-sourcing our code and still **work-in-progress**. So at least initially we won't provide any guarantees as to support of this library.

For quick usage examples see the [Usage](#usage) section below or the notebook in this repo.

For further documentation visit https://lector.readthedocs.io/.

## Installing

While this library is not available yet on pypi, you can easily install it from Github with

```
pip install git+https://github.com/graphext/lector
```

## Usage

The below examples illustrate lector's default bevaviour when reading CSV files. For customization options, check the detailed docs here https://lector.readthedocs.io/.

Let's assume we receive the following CSV file, containing some initial metadata, using the semicolon as separator, having some missing fields, and being encoded in Latin-1:

``` python
csv = """
Some metadata
Some more metadata
id;category;metric;count;text
1234982348728374;a;0.1;1;"This is a text."
;b;0.12;;"Natural language text is different from categorical data."
9007199254740993;a;3.14;3;"The Project · Gutenberg » EBook « of Die Fürstin."
""".encode("ISO-8859-1")
```

<details>
<summary>Pandas</summary>

Trying to read this using `pandas.read_csv(...)` and default arguments only will fail. To find the correct arguments, you'll have to open the CSV in a text editor and manually identify the separator and the initial lines to skip, and then try different encodings until you find one that seems to decode all characters correctly. But even if you then manage to read the CSV, the result may not be what you expected:

``` python
df = pd.read_csv(
    io.BytesIO(csv),
    encoding="ISO-8859-1",
    skiprows=3,
    sep=";",
    index_col=False
)
print(df)
print(df.dtypes)
```

```
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

```

The `category` and `text` columns have been imported with the `object` dtype, which is not particularly useful, but not necessarily a problem either.

Note, however, that numeric-like columns with missing data have been cast to the float type. This may seem merely a nuisance in the case of the `count` column, which could easily be cast to a (nullable) integer type. It is, however, a big problem for the `id` column, since not all integers can be represented exactly by a 64 bit floating type:

``` python
print(df.id.iloc[2])
>> 9007199254740992.0
```

which is not the value `"9007199254740993"` contained in our CSV file. We cannot cast this column to the correct type anymore either (e.g. int64 or string), because the original value is lost. It is also a sneaky problem, because you may not realize you've got wrong IDs, and may produce totally wrong analyses if you use them down the line for joins etc. The only way to import CSV files like this correctly is to inspect essentially all columns and all rows manually in a text editor, choose the best data type manually, and then provide these types via pandas `dtype` argument. This may be feasible if you work with CSVs only sporadically, but quickly becomes cumbersome otherwise.

</details>

<details>
<summary>Arrow</summary>

The `arrow` CSV reader faces exactly the same limitations as `pandas`:

``` python
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
```

It needs the same level of human inspection to identify the correct arguments to read the CSV, and destructively casts IDs to floats (but at least uses a more efficient `string` type where applicable, in contrast to pandas `object` dtype):

```
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
```

Again, the only way to ensure correctness of the parsed CSV is to not use arrow's built-in type inference, but provide the desired type for each column manually.
</details>

### Notebooks

If you installed this package into a brand new environment, and depending on the kind of environment (venv, conda etc.), after installation you may have to register this environment with jupyter for it to show as a kernel in notebooks:

``` bash
ipython kernel install --name [myenv] --user
```

Following this, start jupyter with `jupyter notebook` and it should let you select the kernel containing your lector installation.

### Command line interface

Coming soon...

## Development

To install a local copy for development, including all dependencies for test, documentation and code quality, use the following commands:

``` bash
clone git+https://github.com/graphext/lector
cd lector
pip install -v -e ".[dev]"
pre-commit install
```

The [pre-commit](https://pre-commit.com/) command will make sure that whenever you try to commit changes to this repo code quality and formatting tools will be executed. This ensures e.g. a common coding style, such that any changes to be commited are functional changes only, not changes due to different personal coding style preferences. This in turn makes it either to collaborate via pull requests etc.

To test installation you may execute the [pytest](https://docs.pytest.org/) suite to make sure everything's setup correctly, e.g.:

``` bash
pytest -v .
```

## Documentation

The documentation is created using Sphinx and is available here: https://lector.readthedocs.io/.

You can build and view the static html locally like any other Sphinx project:

``` bash
(cd docs && make clean html)
(cd docs/build/html && python -m http.server)
```


## To Do

- _Parallelize type inference_
  While type inference is already pretty fast, it can potentially be sped up by processing columns in parallel.

## License

This project is licensed under the terms of the Apache License 2.0.

## Links

- Documentation: https://lector.readthedocs.io/
- Source: https://github.com/graphext/lector
- Graphext: https://www.graphext.com
- Graphext on Twitter: https://twitter.com/graphext
