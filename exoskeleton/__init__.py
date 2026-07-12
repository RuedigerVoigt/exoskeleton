"Exoskeleton Crawler Framwork for Python"

import importlib.metadata

from exoskeleton.action_types import ActionType
from exoskeleton.core import Exoskeleton

__all__ = ['ActionType', 'Exoskeleton']

NAME = "exoskeleton"
__version__ = importlib.metadata.version("exoskeleton")
__author__ = "Rüdiger Voigt"
