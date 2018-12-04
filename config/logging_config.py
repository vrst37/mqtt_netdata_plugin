# -*- coding: utf-8 -*-
"""
This file contains the logging configuration
"""

from __future__ import (
    absolute_import, division, print_function, unicode_literals
)

import os.path as osp
import pathlib

from config.error import ConfigException
from structlog import configure, processors, stdlib, threadlocal


def get_logging_conf() -> dict:
    """
    This function returns the logging config as.py a dict
    :return: logging conf
    """
    filename = (
        "/home/as/mosquito_monitor.log"
    )

    dir_name = osp.dirname(osp.normpath(filename))

    pathlib.Path(dir_name).mkdir(parents=True, exist_ok=True)

    try:
        logging_conf = {
            "version": 1,
            "formatters": {
                "simple": {
                    "format": "%(levelname)-6s :: %(name)-5s :: "
                              "%(funcName)-5s :: %(message)s"
                },
                "precise": {
                    "format": "%(asctime)s :: %(levelname)-6s :: %(name)-5s ::"
                              " %(funcName)-5s :: %(message)s"
                },
                'json_formatter': {
                    'format': '%(message)s %(lineno)d '
                              '%(funcName)s %(filename)s',
                    'class': 'pythonjsonlogger.jsonlogger.JsonFormatter'
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "simple"
                },
                'json': {
                    'formatter': 'json_formatter',
                    'backupCount': 4,
                    'class': 'logging.handlers.TimedRotatingFileHandler',
                    'encoding': 'ASCII',
                    'filename': filename,
                    'interval': 1,
                    'when': 'midnight',
                    'level': 'DEBUG'
                }
            },
            "loggers": {
                "MOSQUITO_MONITOR": {
                    "level": "DEBUG",
                    "propagate": "no",
                    "handlers": ["json", "console"]
                },
                "local_mqtt_client.local_mqtt_client": {
                    "level": "DEBUG",
                    "propagate": "no",
                    "handlers": ["json", "console"]
                }
            }
        }
    except SyntaxError as invalid_syntax_exception:
        raise ConfigException(
            "Invalid config provided, {}".format(invalid_syntax_exception)
        )
    else:
        configure(
            context_class=threadlocal.wrap_dict(dict),
            logger_factory=stdlib.LoggerFactory(),
            wrapper_class=stdlib.BoundLogger,
            processors=[
                stdlib.filter_by_level,
                stdlib.add_logger_name,
                stdlib.add_log_level,
                stdlib.PositionalArgumentsFormatter(),
                processors.TimeStamper(fmt='iso'),
                processors.StackInfoRenderer(),
                processors.format_exc_info,
                processors.UnicodeDecoder(),
                stdlib.render_to_log_kwargs,
            ]
        )

        return logging_conf
