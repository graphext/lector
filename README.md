[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/graphext/lector/HEAD?labpath=notebooks%2Fusage.ipynb)

# Lector

[Lector](https://github.com/graphext/lector) aims to be a fast reader for potentially messy CSV files with configurable column type inference. It combines automatic detection of file encodings, CSV dialects (separator, escaping etc.) and preambles (initial lines containing metadata or junk unrelated to the actual tabular data). Its goal is to just-read-the-effing-CSV file without manual configuration in most cases. Each of the detection components is configurable and can be swapped out easily with custom implementations.

Also, since both [pandas](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html) and Apache [Arrow](https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html) will destructively cast columns to the wrong type in some cases (e.g. large ID-like integer strings to floats), it provides an alternative and customisable inference and casting mechanism.

Under the hood it uses pyarrow's [CSV parser](https://arrow.apache.org/docs/python/generated/pyarrow.csv.read_csv.html) for reading, and its [compute functions](https://arrow.apache.org/docs/python/api/compute.html) for optional type inference.

Lector is used at [Graphext](https://www.graphext.com) behind the scenes whenever a user uploads a new dataset, and so implicitly has been validated across 1000s of different CSV files from all kinds of sources. Note, however, that this is Graphext's first foray into open-sourcing our code and still _work-in-progress_. So at least initially we won't provide any guarantees as to support of this library.

For quick usage examples see the [Usage](#usage) section below or the [notebook](notebooks/usage.ipynb) in this repo.

For detailed documentation visit https://lector.readthedocs.io/.

## Installing

While this library is not available yet on pypi, you can easily install it from Github with

```
pip install git+https://github.com/graphext/lector
```

## Usage

Let's assume we receive a CSV file containing some initial metadata, using the semicolon as separator, having some missing fields, and being encoded in Latin-1 (you'd be surprised how common such files are in the real world).

<details>
<summary>Create example CSV file</summary>

``` python
csv = """
Some preamble content here
This is still "part of the metadata preamble"
id;genre;metric;count;content;website;tags;vecs;date
1234982348728374;a;0.1;1;; http://www.graphext.com;"[a,b,c]";"[1.3, 1.4, 1.67]";11/10/2022
;b;0.12;;"Natural language text is different from categorical data."; https://www.twitter.com;[d];"[0, 1.9423]";01/10/2022
9007199254740993;a;3.14;3;"The Project · Gutenberg » EBook « of Die Fürstin.";http://www.google.com;"['e', 'f']";["84.234, 12509.99"];13/10/2021
""".encode("ISO-8859-1")

with open("example.csv", "wb") as fp:
    fp.write(csv)
```
</details>
<br>

To read this with lector into a pandas DataFrame, simply use

``` python
df = lector.read_csv("example.csv", to_pandas=True)
```

Printing the DataFrame and its column types produces the following output:

```
                 id genre  metric  count  \
0  1234982348728374     a    0.10      1
1              <NA>     b    0.12   <NA>
2  9007199254740993     a    3.14      3

                                             content                  website  \
0                                               <NA>  http://www.graphext.com
1  Natural language text is different from catego...  https://www.twitter.com
2  The Project · Gutenberg » EBook « of Die Fürstin.    http://www.google.com

        tags                vecs       date
0  [a, b, c]    [1.3, 1.4, 1.67] 2022-10-11
1        [d]       [0.0, 1.9423] 2022-10-01
2     [e, f]  [84.234, 12509.99] 2021-10-13

id                  Int64
genre            category
metric            float64
count               UInt8
content            string
website          category
tags               object
vecs               object
date       datetime64[ns]
dtype: object
```

This is pretty sweet, because

- we didn't have to tell lector _how_ to read this file (text encoding, lines to skip, separator etc.)
- we didn't have to tell lector the _data types_ of the columns, but it inferred the correct and most efficient ones automatically, e.g.:
    - a nullable `Int64` extension type was necessary to correctly represent values in the `id` column
    - the `category` column was automatically converted to the efficient `dictionary` (categorical) type
    - the `count` column uses the _smallest_ integer type necessary
    - the `text` column, containing natural language text, has _not_ been converted to a categortical type, but kept as string values (it is unlikely to benefit from dictionary-encoding)
    - the `date` column was converted to datetime's correctly, even though the original
      strings are not in an ISO format
    - the `tags` and `vecs` columns have been imported with `object` dtype (since pandas
      doesn't officially support iterables as elements in a column), but its values are in fact numpy array of the correct dtype!

Neither pandas nor arrow will do this. In fact, they cannot even import this data correctly, _without_ attempting to do any smart type inference. Compare e.g. with pandas attempt to read the same CSV file:

<details>
<summary>Pandas and Arrow fail</summary>
Firstly, to get something close to the above, you'll have to spend a good amount of time manually inspecting the CSV file and come up with the following verbose pandas call:

``` python
dtypes = {
    "id": "Int64",
    "genre": "category",
    "metric": "float",
    "count": "UInt8",
    "content": "string",
    "website": "category",
    "tags": "object",
    "vecs": "object"
}

df = pd.read_csv(
    fp,
    encoding="ISO-8859-1",
    skiprows=3,
    sep=";",
    dtype=dtypes,
    parse_dates=["date"],
    infer_datetime_format=True
)

```

While this _parses_ the CSV file alright, the result is, urm, lacking. Let's see:

```
                 id genre  metric  count  \
0  1234982348728374     a    0.10      1
1              <NA>     b    0.12   <NA>
2  9007199254740992     a    3.14      3

                                             content  \
0                                               <NA>
1  Natural language text is different from catego...
2  The Project · Gutenberg » EBook « of Die Fürstin.

                    website        tags                  vecs       date
0   http://www.graphext.com     [a,b,c]      [1.3, 1.4, 1.67] 2022-11-10
1   https://www.twitter.com         [d]           [0, 1.9423] 2022-01-10
2     http://www.google.com  ['e', 'f']  ["84.234, 12509.99"] 2021-10-13

 id                  Int64
genre            category
metric            float64
count               UInt8
content            string
website          category
tags               object
vecs               object
date       datetime64[ns]
dtype: object
```

A couple of observations:

- Pandas _will_ cast numeric columns with missing data to the float type always, before any of our custom types are applied. This is a big problem, as we can see in the `id` column, since not all integers can be represented exactly by a 64 bit floating type (the correct value in our file is `9007199254740993` 👀). It is also a sneaky problem, because this happens silently, and so you may not realize you've got wrong IDs, and may produce totally wrong analyses if you use them down the line for joins etc. The only way to import CSV files like this with pandas correctly is to inspect the actual data in a text editor, guess the best data type, import the data without any type inference, and then individually cast to the correct types. There is no way to configure pandas to import the data correctly.
- Pandas has messed up the dates. While at least warning us about it, pandas doesn't try to infer a consistent date format across all rows. While the CSV file contains all dates in a single consistent format (`%d/%m/%Y`), pandas has used mixed formats and so imported some dates wrongly.
- The `category` and `text` columns have been imported with the `object` dtype, which is not particularly useful, but not necessarily a problem either.
- Since pandas doesn't support iterable dtypes, the tags and vecs columns haven't been parsed into any useful structures

Note that Arrow doesn't fare much better. It doesn't parse and infer its own `list` data type, it doesn't know how to parse dates in any format other than ISO 8601, and commits the same integer-as-float conversion error.
</details>
<br>

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

- _Parallelize type inference_? While type inference is already pretty fast, it can potentially be sped up by processing columns in parallel.
- _Testing_. The current pytest setup is terrible. I've given `hypothesis_csv` a try here,
but I'm probably making bad use of it. Tests are convoluted and probably not even good a catching corner cases.

## License

This project is licensed under the terms of the Apache License 2.0.

## Links

- Documentation: https://lector.readthedocs.io/
- Source: https://github.com/graphext/lector
- Graphext: https://www.graphext.com
- Graphext on Twitter: https://twitter.com/graphext
