"""Provide exceptions related to loading data."""


class DownloadError(Exception):
    """Raise for failures relating to source file downloads."""


class SourceFormatError(Exception):
    """Raise when source data formatting is incompatible with the source transformation
    methods: for example, if columns in a CSV file have changed.
    """
