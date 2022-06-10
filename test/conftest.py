"""Shared fixtures."""
from dataclasses import dataclass
from inspect import cleandoc

import pytest

from lector.csv import Format


@dataclass
class TestCase:
    name: str
    format: Format
    csv: str


@pytest.fixture
def simple_csv() -> TestCase:

    return TestCase(
        name="simple",
        format=Format(),
        csv=cleandoc(
            """
            a,b,c
            0,1,2
            3,4,5
            """
        ),
    )


# t1 = TestCase(
#     "SingleCol-NoHeader",
#     "exp": {"preamble"}
#     """
#     0
#     1
#     2
#     3
#     """,


# DATA = [
#     # ----------------------------------------
#     """0
#     1
#     2
#     3""",
#     0,
#     # ----------------------------------------
#     """abc
#     def
#     ghi
#     jkl""",
#     0,
#     # ----------------------------------------
#     """abc
#     def
#     ghi
#     j,k,l""",
#     3,
#     # ----------------------------------------
#     """abc
#     def
#     ghi
#     j;k;l""",
#     3,
#     # ----------------------------------------
#     """abc
#     def
#     ghi
#     j|k|l""",
#     3,
#     # ----------------------------------------
#     """"abc"
#     def
#     ghi
#     j,k,l""",
#     3,
#     # ----------------------------------------
#     """abc
#     "some, long, texts may have separators"
#     "g,h,i"
#     "j,k,l" """,
#     0,
#     # ----------------------------------------
#     """abc
#     "some; long; texts may have separators"
#     "g,h,i"
#     "j,k,l"
#     """,
#     0,
#     # ----------------------------------------
#     """abc
#     "some; long; texts may have separators"
#     "g,h,i"
#     "j,k,l"
#     0,1,2
#     3,4,5
#     """,
#     4,
#     # ----------------------------------------
#     """random rubbish
#     some invalid ""csv"" here
#     "some ""text,"" in quotes"
#     "some, normal text"
#     text
#     a, b, c""",
#     5,
#     # ----------------------------------------
#     """random rubbish
#     some more rubbish
#     12, "some ""text,"" in quotes", abc
#     13, "some normal text", abc""",
#     2,
#     # ----------------------------------------
#     """random rubbish
#     some more rubbish
#     12, "some "text" in quotes", abc
#     13, "some normal text", abc""",
#     2,
#     # ----------------------------------------
#     """rubbish header
#     more rubbish
#     a, "uahdu
#     asd asda", b""",
#     2,
#     # ----------------------------------------
#     """rubbish header
#     more rubbish
#     "multiline ""text"",
#     continues
#     , and ""ends"" here"
#     single line text
#     abc, def
#     """,
#     6,
#     # ----------------------------------------
#     """rubbish header
#     more rubbish
#     "multiline ""text"",
#     continues
#     , and ""ends"" here", "a"
#     single line text
#     "abc, def", ghi
#     """,
#     2,
# ]
