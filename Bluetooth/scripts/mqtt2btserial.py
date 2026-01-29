import serial
import json
import paho.mqtt.client as mqtt
import time

# Configuring MQTT server and COM
mqtt_ip = input("MQTT Broker IP: ")
if not mqtt_ip:  # If input is empty or None
    mqtt_ip = '192.168.4.20'

mqtt_port = input("MQTT Broker Port: ")
if not mqtt_port:  # If input is empty or None
    mqtt_port = 1883
else:
    mqtt_port = int(mqtt_port)  # Convert to integer

bluetooth_com_port = input("Bluetooth COM (e.g. COM5): ")
if not bluetooth_com_port:  # If input is empty or None
    bluetooth_com_port = 'COM4'

# Debugging logs
print(f'\n-------\nCONFIGURATION\n-------\nIP: {mqtt_ip}\nPORT: {mqtt_port}\nBT COM: {bluetooth_com_port}')

# MQTT callback functions
def on_connect(client, userdata, flags, rc):
    print(f"Connected, status = {str(rc)}")

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.payload))


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connecting to serial
print("Connecting to serial: " + bluetooth_com_port)
time.sleep(1)
ser = serial.Serial(bluetooth_com_port, 9600)
print("Bluetooth COM opened")

# Ensure MQTT connection
def ensure_mqtt_connection():
    try:
        # Connect to MQTT broker only once, no retrying every loop
        client.connect(mqtt_ip, mqtt_port, 60)
        client.loop_start()  # Start the loop to process network traffic
        print(f"Connected to MQTT Broker at {mqtt_ip}:{mqtt_port}")
    except Exception as e:
        print(f"Failed to connect to MQTT: {e}")
        time.sleep(2)  # Retry after delay

ensure_mqtt_connection()

while True:

    serial_json_string = ser.readline().decode('utf-8', errors='ignore').strip()

    if not serial_json_string:
        continue

    print("RAW:", serial_json_string)

    # Only process JSON messages
    if not serial_json_string.startswith('{'):
        print("Skipping non-JSON message")
        continue

    try:
        sensor_data = json.loads(serial_json_string)
        print(f"Received from Bluetooth: {sensor_data}")

        group_name = list(sensor_data.keys())[0]
        device_id = list(sensor_data[group_name])[0]

        for key, val in sensor_data[group_name][device_id].items():
            success = client.publish(
                f'{group_name}/{device_id}/{key}',
                str(val).encode("UTF-8")
            )

            print(f'MQTT Publish {group_name}/{device_id}/{key} -> {val}')

    except Exception as e:
        print("Failed to decode:", e)
        print("BAD DATA:", serial_json_string)

ser.close()
