"""Test CSV readers.

For future reference, if we want to make tests extensible to future implementations
of some of the interfaces (ABCs) in this package, we can use parameterized fixtures,
like this:

- https://github.com/pytest-dev/pytest/issues/421
- https://stackoverflow.com/q/26266481/3519145

"""
# import io

# import pytest
# from hypothesis import given
# from hypothesis.strategies import data
# from hypothesis_csv.strategies import csv as make_csv

# from lector.csv import ArrowReader, EmptyFileError

# SHAPES = [
#     # (0, 1), # hypothesis_csv cannot generate a 0 row file
#     # (1, 0), # This produces an empty file ("\r\n\r\n")
#     (1, 1),
#     (1, 3),
#     (3, 1),
#     (10, 5),
# ]
# """Different combinations of n_rows and n_cols."""

# EMPTY = ["", "\n", "\n\n", "\r\n"]


# def arrow_reader(s: str):
#     fp = io.BytesIO(s.encode("utf-8"))
#     return ArrowReader(fp, log=False)


# @pytest.mark.parametrize("csv", EMPTY)
# def test_empty(csv: str):
#     """Correct number of columns and rows."""
#     with pytest.raises(EmptyFileError):
#         arrow_reader(csv).read()


# @given(data=data())
# @pytest.mark.parametrize("shape", SHAPES)
# def test_parsed_shapes(shape, data):
#     n_rows, n_cols = shape
#     strategy = make_csv(lines=n_rows, header=n_cols)
#     csv = data.draw(strategy)
#     tbl = arrow_reader(csv).read()

#     assert len(tbl) == n_rows
#     assert tbl.num_columns == n_cols
