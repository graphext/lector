from lector.log import LOG


def equal(obj1, obj2, extra=None):
    """Helper to print useful info if result is unexpected."""
    eq = obj1 == obj2

    if not eq:
        LOG.print(obj1)
        LOG.print(obj2)

        if extra is not None:
            LOG.print(extra)

        return False

    return True
