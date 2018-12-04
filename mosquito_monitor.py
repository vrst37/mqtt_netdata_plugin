# -*- coding: utf-8 -*-
"""
This file implements the main code monitoring mosquitto
"""

from __future__ import (
    absolute_import, division, print_function, unicode_literals
)

import logging.config

import structlog

from distutils.version import StrictVersion

from config import logging_config
from config.service_name import MICROSERVICE_NAME
from local_mqtt_client.local_mqtt_client import LocalMQTTClient

version = StrictVersion("1.0.1")

# load the config config
logging.config.dictConfig(logging_config.get_logging_conf())
mosquito_monitor_logger = structlog.getLogger(MICROSERVICE_NAME)
mosquito_monitor_logger.addHandler(logging.NullHandler())

mosquito_monitor_logger.info("Starting the Monitor")

lbc = LocalMQTTClient()

mosquito_monitor_logger.info("UpstreamMQTTClient Object Created")

lbc.run_loop(in_thread=False, forever=True)
