"""Common regex patterns used in mutiple modules."""

RE_INT = "^(?P<sign>[+-])?(?P<num>[0-9]+)$"
"""Capture optional sign and numeric parts in integer strings."""

RE_IS_FLOAT = "^[-]?[0-9]*[.]?[0-9]*(?:[e][+-]?[0-9]+)?$"
"""Strings matching float representations convertable by Arrow. Allows ints too,
but those should have been inferred before trying floats.
"""

RE_LIST_LIKE: str = r"^[\(\[\|\{<].*[\)\]\|\>}]$"
"""Lists start and end with parenthesis-like characters."""

RE_LIST_CLEAN: str = r"^[\[\{\(\|<]|[\]\}\)\|>]$|['\"\s]"
"""Remove all parenthesis-like characters from start and end. Whitespace and quotes too."""

RE_LIST_PARENS: str = r"^[\[\{\(\|<]|[\]\}\)\|>]$"
"""Remove all parenthesis-like characters from start and end."""

RE_URL = (
    r"^(http://www\.|https://www\.|http://|https://)?"  # http:// or https://
    r"(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|"  # domain...
    r"localhost|"  # localhost...
    r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"  # ...or ip
    r"(?::\d+)?"  # optional port
    r"(?:/?|[/?]\S+)$"
)

RE_TRAILING_DECIMALS: str = r"\.(\d+)$"
"""Strictly trailing, i.e. nothing after the decimals."""

RE_FRATIONAL_SECONDS: str = r"(?P<frac>\.\d+)"
"""Allows for timezone after fractional seconds, capturing part to be replaced."""
