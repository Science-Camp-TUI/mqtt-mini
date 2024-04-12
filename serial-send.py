import datetime
import json
import time
import random

import serial
import struct

FIXED_MESSAGE_LENGTH = 24

if __name__ == "__main__":

    num_ids = 6000

    ser = serial.Serial('COM4', 115200, timeout=1)
    sim_time_step_secs = 3

    try:
        while True:
            # we are using a very simple protocol here to reduce the amount of data sent via serial / Mioty
            # generate random classification result
            bird_id = random.randint(0, num_ids)
            bird_confidence = random.random()
            random_lat = random.uniform(-90, 90)
            random_lon = random.uniform(-180, 180)
            date_time = datetime.datetime.now()
            timestamp_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            timestamp = date_time.timestamp()
            data = {
                "class_id": bird_id,
                "confidence": f"{bird_confidence:.2f}",
                "lat": f"{random_lat:.4f}",
                "lon": f"{random_lon:.4f}",
                "timestamp": timestamp_str
            }

            send_str = json.dumps(data, ensure_ascii=True)
            send_str += "\0"
            binary_data_json = bytes(send_str, "utf-8")

            data_list = [bird_id, bird_confidence, random_lat, random_lon, timestamp]
            binary_data_struct = struct.pack('Hfffd', *data_list)

            if len(binary_data_struct) != FIXED_MESSAGE_LENGTH:
                print(f"Error: Message size does not match: {len(binary_data_struct)} <> {FIXED_MESSAGE_LENGTH} bytes")
            else:
                print(binary_data_struct.hex(sep=" "))
                print(f"{data}")
                ser.write(binary_data_struct)

            time.sleep(sim_time_step_secs)

    except KeyboardInterrupt:
        ser.close()
        print("Serial port closed")
