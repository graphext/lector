from csv import QUOTE_MINIMAL

from rich.pretty import pprint as print


def equal(obj1, obj2):
    eq = obj1 == obj2
    if not eq:
        print(obj1)
        print(obj2)
        return False
    return True


def fix_expected_dialect(dialect):
    dialect.line_terminator = "\r\n"  # Hardcoded in sniffer (not detectable)
    dialect.quoting = QUOTE_MINIMAL  # Hardcoded in sniffer (not detectable)
    return dialect
