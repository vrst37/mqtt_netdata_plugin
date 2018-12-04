# -*- coding: utf-8 -*-
"""
This file implements the local broker client object
"""

import logging.config
from statsd import StatsClient

import paho.mqtt.client as mqtt
from config import logging_config
import structlog
from config.mqtt_config import LOCAL_MQTT_PORT, LOCAL_MQTT_ADDRESS
from config.statsd_config import STATSD_PORT, STATSD_ADDRESS


class LocalMQTTClient:
    """
    This class connects to the local broker and interacts with it as needed
    """
    def __init__(
            self, username: str=None, password: str=None
    ):
        logging.config.dictConfig(logging_config.get_logging_conf())
        self._logger = structlog.getLogger(__name__)
        self._logger.addHandler(logging.NullHandler())

        # we are now using a retain session to get all missed messages in
        # case we disconnect
        self._client = mqtt.Client(
            clean_session=True, userdata=None,
            protocol=mqtt.MQTTv311
        )

        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self.on_message

        self._set_message_callbacks()

        self._client.username_pw_set(
            username=str(username), password=str(password)
        )

        # run the connect function
        self._connect()
        self._stats_client = StatsClient(host=STATSD_ADDRESS, port=STATSD_PORT)
        self._logger.info("Local MQTT Client init called")

    def _set_message_callbacks(self):
        """

        :return:
        """
        self._client.message_callback_add(
            "$SYS/broker/bytes/received", self.on_bytes_received
        )

        self._client.message_callback_add(
            "$SYS/broker/bytes/sent", self.on_bytes_sent
        )
        self._client.message_callback_add(
            "$SYS/broker/clients/connected", self.on_clients_connected
        )
        self._client.message_callback_add(
            "$SYS/broker/clients/expired", self.on_clients_expired
        )
        self._client.message_callback_add(
            "$SYS/broker/clients/disconnected", self.on_clients_disconnected
        )
        self._client.message_callback_add(
            "$SYS/broker/clients/maximum", self.on_clients_maximum
        )
        self._client.message_callback_add(
            "$SYS/broker/clients/total", self.on_clients_total
        )
        self._client.message_callback_add(
            "$SYS/broker/heap/current", self.on_heap_current_size
        )
        self._client.message_callback_add(
            "$SYS/broker/heap/maximum", self.on_heap_maximum_size
        )
        self._client.message_callback_add(
            "$SYS/broker/load/connections/1min", self.on_connections_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/connections/5min", self.on_connections_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/connections/15min", self.on_connections_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/bytes/received/1min", self.on_bytes_received_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/bytes/received/5min", self.on_bytes_received_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/bytes/received/15min",
            self.on_bytes_received_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/bytes/sent/1min", self.on_bytes_sent_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/bytes/sent/5min", self.on_bytes_sent_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/bytes/sent/15min", self.on_bytes_sent_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/messages/received/1min",
            self.on_messages_received_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/messages/received/5min",
            self.on_messages_received_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/messages/received/15min",
            self.on_messages_received_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/messages/sent/1min", self.on_messages_sent_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/messages/sent/5min", self.on_messages_sent_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/messages/sent/15min", self.on_messages_sent_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/dropped/1min",
            self.on_publish_dropped_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/dropped/5min",
            self.on_publish_dropped_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/dropped/15min",
            self.on_publish_dropped_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/received/1min",
            self.on_publish_received_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/received/5min",
            self.on_publish_received_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/received/15min",
            self.on_publish_received_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/sent/1min", self.on_publish_sent_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/sent/5min", self.on_publish_sent_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/publish/sent/15min", self.on_publish_sent_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/sockets/1min", self.on_sockets_1min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/sockets/5min", self.on_sockets_5min
        )
        self._client.message_callback_add(
            "$SYS/broker/load/sockets/15min", self.on_sockets_15min
        )
        self._client.message_callback_add(
            "$SYS/broker/messages/inflight", self.on_inflight
        )
        self._client.message_callback_add(
            "$SYS/broker/messages/received", self.on_messages_received
        )
        self._client.message_callback_add(
            "$SYS/broker/messages/sent", self.on_messages_sent
        )
        self._client.message_callback_add(
            "$SYS/broker/messages/stored", self.on_messages_stored
        )
        self._client.message_callback_add(
            "$SYS/broker/publish/messages/dropped", self.on_publish_dropped
        )
        self._client.message_callback_add(
            "$SYS/broker/publish/messages/received", self.on_publish_received
        )
        self._client.message_callback_add(
            "$SYS/broker/publish/messages/sent", self.on_publish_sent
        )
        self._client.message_callback_add(
            "$SYS/broker/retained messages/count", self.on_retain_messages_count
        )
        self._client.message_callback_add(
            "$SYS/broker/subscriptions/count", self.on_subscription_count
        )
        self._client.message_callback_add(
            "$SYS/broker/uptime", self.on_broker_uptime
        )

    # The callback for when a PUBLISH message is received from the server.
    def on_message(self, client, userdata, msg):
        self._logger.info("Received nessage", mqtt_msg=msg)

    def _connect(self) -> None:
        """
        This function calls the connect function on the client object,
        this will block till there is an ok connection
        :return: None
        """
        # checking if connection is ok (first we assume connection is not ok)
        try:
            self._logger.info(
                "Attempting to connect to local MQTT server.",
                server=LOCAL_MQTT_ADDRESS, port=LOCAL_MQTT_PORT, kepalive=60
            )

            self._client.connect_async(
                host=LOCAL_MQTT_ADDRESS, port=LOCAL_MQTT_PORT, keepalive=60
            )

        except Exception as e:
            # exception was raised during the connect function, we must wait
            # for ok connection
            self._logger.error("Unable to connect to MQTT Broker", error=e)

    def run_loop(
            self, timeout: float=.25, in_thread: bool=False, loop_var: int=1,
            forever: bool=False
    ) -> None:
        """
        This function starts the loop in a separate thread for this object.
        :param timeout: passes the timeout to the loop function
        :param in_thread: if this is set true, then timeout and loop_var is
        ignored and a new thread is launched
        :param loop_var: use this variable to call loop with timeout n number
        of times
        :param forever: if used, then  the function blocks, all other
        parameters are ignored
        :return: None
        """
        try:
            if forever is True:
                self._client.loop_forever()

            if in_thread is False:
                for i in range(0, loop_var):
                    self._client.loop(timeout=timeout)
            else:
                # in thread is set to True, we should start an asynchronous loop
                self._logger.info("Running MQTT loop in a separate loop")
                self._client.loop_start()
        except Exception as e:
            self._logger.error("Error starting the loop", error=e)
            raise e

    def _on_connect(self, client, userdata, flags, rc):
        """
        This function executed on on_connect, Args are left blank because
        they are predetermined
        :param client:
        :param userdata:
        :param flags:
        :param rc:
        :return:
        """
        self._logger.info("Connection to local MQTT server made")

        self._subscribe()

    def _subscribe(self) -> None:
        """
        This function subscribes to provided topic
        :return: None
        """
        self._logger.info("Subscribing to endpoint", ep="$SYS/#")
        self._client.subscribe("$SYS/#", qos=1)

    def _on_disconnect(self, client, userdata, rc) -> None:
        """
        This function is called on disconnect event
        :param client: the client object
        :param userdata: the data set by user on startup
        :param rc: The return error code
        :return: None
        """
        self._logger.info(
            "Local MQTT Client has disconnected from the MQTT Server",
            userdata=userdata, rc=rc, rc_string=mqtt.error_string(rc)
        )

    def on_bytes_received(self, client, userdata, msg):
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_received', float(msg.payload)
        )

    def on_bytes_sent(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_sent', float(msg.payload)
        )

    def on_clients_connected(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.clients_connected', float(msg.payload)
        )

    def on_clients_expired(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.clients_expired', float(msg.payload)
        )

    def on_clients_disconnected(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.clients_disconnected', float(msg.payload)
        )

    def on_clients_maximum(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.clients_maximum', float(msg.payload)
        )

    def on_clients_total(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.clients_total', float(msg.payload)
        )

    def on_heap_current_size(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.heap_current', float(msg.payload)
        )

    def on_heap_maximum_size(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.heap_maximum', float(msg.payload)
        )

    def on_connections_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.connections_1min', float(msg.payload)
        )

    def on_connections_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.connections_5min', float(msg.payload)
        )

    def on_connections_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.connections_15min', float(msg.payload)
        )

    def on_bytes_received_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_received_1min', float(msg.payload)
        )

    def on_bytes_received_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_received_5min', float(msg.payload)
        )

    def on_bytes_received_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_received_15min', float(msg.payload)
        )

    def on_bytes_sent_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_sent_1min', float(msg.payload)
        )

    def on_bytes_sent_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_sent_5min', float(msg.payload)
        )

    def on_bytes_sent_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.bytes_sent_15min', float(msg.payload)
        )

    def on_messages_received_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_received_1min', float(msg.payload)
        )

    def on_messages_received_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_received_5min', float(msg.payload)
        )

    def on_messages_received_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_received_15min', float(msg.payload)
        )

    def on_messages_sent_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_sent_1min', float(msg.payload)
        )

    def on_messages_sent_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_sent_5min', float(msg.payload)
        )

    def on_messages_sent_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_sent_15min', float(msg.payload)
        )

    def on_publish_dropped_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_dropped_1min', float(msg.payload)
        )

    def on_publish_dropped_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_dropped_5min', float(msg.payload)
        )

    def on_publish_dropped_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_dropped_15min', float(msg.payload)
        )

    def on_publish_received_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_received_1min', float(msg.payload)
        )

    def on_publish_received_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_received_5min', float(msg.payload)
        )

    def on_publish_received_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_received_15min', float(msg.payload)
        )

    def on_publish_sent_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_sent_1min', float(msg.payload)
        )

    def on_publish_sent_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_sent_5min', float(msg.payload)
        )

    def on_publish_sent_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_sent_15min', float(msg.payload)
        )

    def on_sockets_1min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.sockets_1min', float(msg.payload)
        )

    def on_sockets_5min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.sockets_5min', float(msg.payload)
        )

    def on_sockets_15min(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.sockets_15min', float(msg.payload)
        )

    def on_inflight(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.inflight', float(msg.payload)
        )

    def on_messages_received(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_received', float(msg.payload)
        )

    def on_messages_sent(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_sent', float(msg.payload)
        )

    def on_messages_stored(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.messages_stored', float(msg.payload)
        )

    def on_publish_dropped(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_dropped', float(msg.payload)
        )

    def on_publish_received(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_received', float(msg.payload)
        )

    def on_publish_sent(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.publish_sent', float(msg.payload)
        )

    def on_retain_messages_count(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.retain_messages_count', float(msg.payload)
        )

    def on_subscription_count(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.subscription_count', float(msg.payload)
        )

    def on_broker_uptime(self, client, userdata, msg) -> None:
        """

        :param client:
        :param userdata:
        :param msg:
        :return:
        """
        self._logger.info(
            "Received SYS message", topic=msg.topic, payload=msg.payload
        )
        self._stats_client.gauge(
            'mosquito_monitor.broker_uptime', float(msg.payload.split(b' ')[0])
        )
