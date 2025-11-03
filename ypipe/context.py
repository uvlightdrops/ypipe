from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class Context(dict):
    """The ypipe context.
    """

    def __init__(self, *args, **kwargs):
        """Initialize context."""
        super().__init__(*args, **kwargs)


