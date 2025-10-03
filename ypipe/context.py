
class Context(dict):
    """The ypipe context.
    """

    def __init__(self, *args, **kwargs):
        """Initialize context."""
        super().__init__(*args, **kwargs)