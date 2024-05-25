import datetime
import json
import ssl
import struct

import paho.mqtt.client as mqtt

from mqtt_mini.config import *


def on_subscribe(client, userdata, mid, reason_code_list, properties):
    if reason_code_list[0].is_failure:
        print(f"Broker rejected you subscription: {reason_code_list[0]}")
    else:
        print(f"Broker granted the following QoS: {reason_code_list[0].value}")


def on_unsubscribe(client, userdata, mid, reason_code_list, properties):
    if len(reason_code_list) == 0 or not reason_code_list[0].is_failure:
        print("unsubscribe succeeded (if SUBACK is received in MQTTv3 it success)")
    else:
        print(f"Broker replied with failure: {reason_code_list[0]}")
    client.disconnect()


def on_message(client, userdata, message):
    try:
        userdata["message_handler"](message, userdata["message_length"])
    except KeyError:
        print(f"Error: no message handler set!")


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        print(f"Failed to connect: {reason_code}. loop_forever() will retry connection")
    else:
        print(f"Connected with result code {reason_code} - Subscribing to topic:")
        for topic in userdata["topics"]:
            print(f"|-- {topic}")
            client.subscribe(topic)


def make_dict(data_bytes):
    """
    Converts simple data stored in bytes back to a meaningful json object
    Data order is
    [bird_class_id, bird_confidence, latitude, longitude, timestamp]
    [uint_16, float32, float32, float32, float32]
    :param data_bytes:
    :return:
    """
    data_values = struct.unpack('Hfffd', data_bytes)

    datetime_value = datetime.datetime.fromtimestamp(data_values[4])
    timestamp_str = datetime_value.strftime("%Y-%m-%d %H:%M:%S")
    struct_data = {
        "class_id": data_values[0],
        "confidence": f"{data_values[1]:.2f}",
        "lat": f"{data_values[2]:.4f}",
        "lon": f"{data_values[3]:.4f}",
        "timestamp": timestamp_str
    }
    print(struct_data)


def process_message(message, message_length):
    topic_str = message.topic.split("/")[-1]
    recv_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        decoded = json.loads(message.payload.decode())
        data = decoded["data"]
        #pkg_count = decoded["cnt"]
        data_bytes = bytes(data)
        print(data_bytes.hex(sep=" "))
        if len(data_bytes) != message_length:
            print(f"ERROR: Message size does not match: {len(data_bytes)} <> {message_length} bytes - skipping")
            return
    except json.JSONDecodeError:
        print(f"ERROR: Could decode json from MiOTY message")
    except KeyError:
        print(f"ERROR: Could find data field in message")
    else:
        print(f"Got message on topic {topic_str} at {recv_timestamp}:")
        make_dict(data_bytes)


if __name__ == "__main__":

    # this is required since e-technik server uses self-signed certificates
    if use_ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    # create the client
    mqttc = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message
    mqttc.on_subscribe = on_subscribe
    mqttc.on_unsubscribe = on_unsubscribe

    mqttc.username_pw_set(username=mqtt_username, password=mqtt_password)

    if use_ssl:
        mqttc.tls_set_context(ssl_context)
        mqttc.tls_insecure_set(True)

    # we store our topic and our message handler function as user data in the client
    mqttc.user_data_set({
        "topics": mqtt_topics,
        "message_handler": process_message,
        "message_length": 24
    })

    try:
        mqttc.connect(mqtt_host, mqtt_port)
        print("Entering main loop")
        mqttc.loop_forever()
    except KeyboardInterrupt:
        mqttc.disconnect()
        print("Disconnected")
        pass
