"""
Single source of truth for the action types exoskeleton can execute.
~~~~~~~~~~~~~~~~~~~~~
Every value written to queue.action and fileVersions.actionAppliedID is a
foreign key to actions.id, and every fileVersions.storageTypeID a foreign
key to storageTypes.id. These enums define those ids together with the
metadata used to seed both reference tables (see database_schema_check.py),
so the code can never write an id that has no matching row.

Adding a new action means adding a member to ActionType here and
registering a handler class in actions.py (ExoActions.ACTION_HANDLERS).

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

from enum import IntEnum


class StorageTypeId(IntEnum):
    """Storage type ids stored in fileVersions.storageTypeID / storageTypes.id.

    Members carry the row data used to seed the storageTypes table:
        value       -> storageTypes.id
        .short_name -> storageTypes.shortName
        .full_name  -> storageTypes.fullName

    Ids and names match the reference data shipped with exoskeleton 1.x/2.x
    (1 = Database, 2 = Filesystem), so databases created by released
    versions keep their meaning. Databases from those versions may contain
    additional rows (cloud buckets, ids 3-6) that are not seeded anymore
    but stay untouched.
    """

    # Annotation-only names: declared for type checkers, not enum members.
    short_name: str
    full_name: str

    # Defaults only satisfy type checkers for value lookups like
    # StorageTypeId(2); member creation always passes all arguments.
    def __new__(cls, value: int, short_name: str = '',
                full_name: str = '') -> "StorageTypeId":
        obj = int.__new__(cls, value)
        obj._value_ = value
        obj.short_name = short_name
        obj.full_name = full_name
        return obj

    DATABASE = (1, 'Database', 'Local Database')
    FILESYSTEM = (2, 'Filesystem', 'Local Filesystem')


class ActionType(IntEnum):
    """Action ids stored in queue.action / fileVersions.actionAppliedID.

    Members carry the row data used to seed the actions table plus the
    storage type each action writes its result to:
        value         -> actions.id
        .description  -> actions.description
        .storage_type -> storageTypeID of the FileVersion the action creates
    """

    # Annotation-only names: declared for type checkers, not enum members.
    description: str
    storage_type: StorageTypeId

    # Defaults only satisfy type checkers for value lookups like
    # ActionType(2); member creation always passes all arguments.
    def __new__(cls, value: int, description: str = '',
                storage_type: StorageTypeId = StorageTypeId.DATABASE
                ) -> "ActionType":
        obj = int.__new__(cls, value)
        obj._value_ = value
        obj.description = description
        obj.storage_type = storage_type
        return obj

    DOWNLOAD_FILE = (1, 'Download file to disk', StorageTypeId.FILESYSTEM)
    SAVE_PAGE_CODE = (2, 'Save page code into database',
                      StorageTypeId.DATABASE)
    # PDFs are rendered by a headless browser and stored as files on disk.
    PAGE_TO_PDF = (3, 'Save page as PDF using headless browser',
                   StorageTypeId.FILESYSTEM)
    SAVE_PAGE_TEXT = (4, 'Save page text into database',
                      StorageTypeId.DATABASE)
