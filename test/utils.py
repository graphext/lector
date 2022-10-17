from lector.log import CONSOLE


def equal(obj1, obj2, extra=None):
    """Helper to print useful info if result is unexpected."""
    eq = obj1 == obj2

    if not eq:
        CONSOLE.print(obj1)
        CONSOLE.print(obj2)

        if extra is not None:
            CONSOLE.print(extra)

        return False

    return True
