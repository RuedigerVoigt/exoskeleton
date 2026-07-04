"""
Single source of truth for the error types exoskeleton records.
~~~~~~~~~~~~~~~~~~~~~
Every value written to Queue.causesError is a foreign key to errorType.id.
This enum defines those ids together with the metadata used to seed the
errorType table (see database_schema_check.py), so the code can never write
an id that has no matching row.

Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

from enum import IntEnum


class ErrorCode(IntEnum):
    """Error type ids stored in Queue.causesError / errorType.id.

    Members carry the row data used to seed the errorType table:
        value       -> errorType.id
        .short       -> errorType.short (<= 31 chars)
        .description -> errorType.description
        .permanent   -> errorType.permanent (True = not retried)

    HTTP status codes double as their own ids. Non-HTTP failures use small
    ids below 400 so they cannot collide with a status code.
    """

    # Annotation-only names: declared for type checkers, not enum members.
    short: str
    description: str
    permanent: bool

    def __new__(cls, value: int, short: str,
                description: str, permanent: bool) -> "ErrorCode":
        obj = int.__new__(cls, value)
        obj._value_ = value
        obj.short = short
        obj.description = description
        obj.permanent = permanent
        return obj

    # ~~~ Internal (non-HTTP) error types ~~~
    GAVE_UP = (3, 'gave_up', 'Gave up after too many retries', True)
    TIMEOUT = (4, 'conn_timeout', 'Connection or read timed out', False)
    PDF_PROCESS_ERROR = (5, 'pdf_process_error',
                         'Headless browser failed to render PDF', False)
    UNKNOWN = (6, 'unknown', 'Unknown error while processing task', False)
    # Storage failure after a successful download: marked permanent so the
    # item is not re-downloaded in an endless loop. Clear with
    # forget_permanent_errors() to retry.
    STORAGE_FAILED = (7, 'storage_failed',
                      'Failed to store downloaded content', True)

    # ~~~ Permanent HTTP status codes ~~~
    BAD_REQUEST = (400, 'bad_request', 'Bad Request', True)
    UNAUTHORIZED = (401, 'unauthorized', 'Unauthorized', True)
    PAYMENT_REQUIRED = (402, 'payment_required', 'Payment Required', True)
    FORBIDDEN = (403, 'forbidden', 'Forbidden', True)
    NOT_FOUND = (404, 'not_found', 'Not Found', True)
    METHOD_NOT_ALLOWED = (405, 'method_not_allowed', 'Method Not Allowed', True)
    NOT_ACCEPTABLE = (406, 'not_acceptable', 'Not Acceptable', True)
    PROXY_AUTH = (407, 'proxy_auth', 'Proxy Authentication Required', True)
    GONE = (410, 'gone', 'Gone', True)
    UNAVAILABLE_LEGAL = (451, 'unavailable_legal',
                         'Unavailable For Legal Reasons', True)
    NOT_IMPLEMENTED = (501, 'not_implemented', 'Not Implemented', True)

    # ~~~ Temporary HTTP status codes ~~~
    REQUEST_TIMEOUT = (408, 'timeout', 'Request Timeout', False)
    RATE_LIMIT = (429, 'rate_limit', 'Too Many Requests', False)
    SERVER_ERROR = (500, 'server_error', 'Internal Server Error', False)
    BAD_GATEWAY = (502, 'bad_gateway', 'Bad Gateway', False)
    SERVICE_UNAVAILABLE = (503, 'service_unavailable',
                           'Service Unavailable', False)
    GATEWAY_TIMEOUT = (504, 'gateway_timeout', 'Gateway Timeout', False)
    BANDWIDTH_EXCEEDED = (509, 'bandwidth_exceeded',
                          'Bandwidth Limit Exceeded', False)
    SITE_OVERLOADED = (529, 'site_overloaded', 'Site is overloaded', False)
    NETWORK_READ_TIMEOUT = (598, 'network_read_timeout',
                            'Network read timeout error', False)
