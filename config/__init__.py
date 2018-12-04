# -*- coding: utf-8 -*-
"""
The init file for config package
"""

from __future__ import (
    absolute_import, division, print_function, unicode_literals
)

from config import logging_config
from config.error import ConfigException


__all__ = list()
__all__.append('logging_config')
__all__.append('ConfigException')

__version__ = '1.0.0'

__author__ = 'asaxena'
