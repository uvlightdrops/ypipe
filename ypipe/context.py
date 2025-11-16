from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class Context(dict):
    """The ypipe context.
    """

    def __init__(self, *args, **kwargs):
        """Initialize context."""
        super().__init__(*args, **kwargs)
        if 'frames' not in self:
            self['frames'] = {}
        if 'frame_groups' not in self:
            self['frame_groups'] = {}

    def store_frame(self, group, df):
        """Speichert ein DataFrame unter dem gegebenen Gruppennamen."""
        self['frames'][group] = df

    def get_frame(self, group):
        """Gibt das DataFrame für den Gruppennamen zurück."""
        return self['frames'].get(group)

    def store_frame_group(self, frame_group_name, fg_dict):
        """Speichert eine FrameGroup (dict von DataFrames) unter dem Namen."""
        logger.debug("Storing frame group '%s' with keys: %s", frame_group_name, fg_dict.keys())
        self['frame_groups'][frame_group_name] = fg_dict

    def get_frame_group(self, frame_group_name):
        """Gibt die FrameGroup für den Namen zurück."""
        logger.debug("Retrieving frame group '%s'", frame_group_name)
        return self['frame_groups'].get(frame_group_name)

    def copy(self):
        """Gibt eine flache Kopie als Context-Objekt zurück."""
        return Context(self)
