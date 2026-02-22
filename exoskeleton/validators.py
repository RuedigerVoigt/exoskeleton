"""
Pydantic v2 models for exoskeleton constructor arguments.

These models serve two purposes that userprovided does not cover:
  1. Declaring defaults in one place instead of scattering .get('key', default)
     calls across multiple manager classes.
  2. Cross-field validation (e.g. wait_min must not exceed wait_max).

Individual field validation (ranges, types, allowed keys) is still handled
by userprovided as before.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""

from typing import Optional

from pydantic import BaseModel, model_validator


class BotBehavior(BaseModel):
    """Structured defaults and cross-field validation for bot_behavior."""

    connection_timeout: int = 60
    queue_max_retries: int = 3
    queue_revisit: int = 20
    rate_limit_wait: int = 1860
    stop_if_queue_empty: bool = False
    wait_min: int = 5
    wait_max: int = 30

    @model_validator(mode='after')
    def wait_min_must_not_exceed_wait_max(self) -> 'BotBehavior':
        if self.wait_min > self.wait_max:
            raise ValueError(
                f'wait_min ({self.wait_min}) must not exceed '
                f'wait_max ({self.wait_max}).')
        return self


class MailBehavior(BaseModel):
    """Structured defaults for mail_behavior."""

    milestone_num: Optional[int] = None
    send_start_msg: bool = True
    send_finish_msg: bool = False
