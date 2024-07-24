import os
import datetime
import random
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
import numpy as np
import asyncio

def random_hex_string(length=4):
    return os.urandom(length).hex()

def generate_measurement(name, timestamp, length):
    return {
        "measurement": name,
        "time": timestamp,
        "fields": {f"f-{i}": random.uniform(0.1, 100.0) for i in range(length)}
    }

def get_pump_count():
    pump_count = int(os.getenv('PUMPCOUNT', 20))
    return max(pump_count, 1)

def generate_pump(number):
    lauf = f"-LC{number}"
    return [(random_hex_string() + lauf, 1) for _ in range(7)] + \
           [(random_hex_string() + lauf, 30) for _ in range(3)] + \
           [(random_hex_string() + lauf, 5) for _ in range(2)]

def generate_pump_list(pump_count):
    return [generate_pump(i) for i in range(pump_count)]

def print_log(output, text):
    if output:
        print(text)

async def main():
    print("Starting main function")
    bucket = os.environ['BUCKET']
    org = os.environ['ORG']
    token = os.environ['TOKEN']
    url = os.environ['URL']

    client = InfluxDBClient(url=url, token=token, org=org)
    write_options = WriteOptions(
        batch_size=5000,             # Große Batch Size, um die Anzahl der HTTP-Anfragen zu reduzieren
        flush_interval=3000,         # Flush Interval von 3 Sekunden, um die Latenz zu verringern
        jitter_interval=500,         # Jitter Interval, um Überlastungen zu vermeiden
        retry_interval=1500,         # Retry Interval von 1,5 Sekunden
        max_retries=5,               # Maximale Anzahl von 5 Wiederholungsversuchen
        max_retry_delay=7000,        # Maximale Verzögerung von 7 Sekunden zwischen den Wiederholungsversuchen
        exponential_base=2           # Exponentielle Basis von 2 für das Wachstum der Verzögerung
    )

    write_api = client.write_api(write_options=write_options)
    log = False  # Set to True for debugging
    pump_count = get_pump_count()
    pump_list = generate_pump_list(pump_count)

    while True:
        dt = datetime.datetime.now()
        tasks = []

        for pump in pump_list:
            tasks.append(process_pump(write_api, bucket, org, pump, dt, log))

        await asyncio.gather(*tasks)
        await asyncio.sleep(1)  # Hauptschleife wird alle 1 Sekunde durchlaufen

async def process_pump(write_api, bucket, org, pump, dt, log):
    tasks = []‘
    for sensor in pump:
        tasks.append(process_sensor(write_api, bucket, org, sensor, dt, log))
    await asyncio.gather(*tasks)

async def process_sensor(write_api, bucket, org, sensor, dt, log):
    records = [generate_measurement(sensor[0], dt, sensor[1]) for _ in range(sensor[1])]
    
    if sensor[1] == 1:
        await asyncio.sleep(abs(np.random.normal(1, 0.2)))  # Normal distribution for 1 Hz
    elif sensor[1] == 30:
        await asyncio.sleep(abs(np.random.exponential(1/30)))  # Exponential distribution for 30 Hz
    elif sensor[1] == 5:
        await asyncio.sleep(abs(np.random.normal(1/5, 0.1)))  # Normal distribution for 5 Hz

    b_w = datetime.datetime.now()
    write_api.write(bucket, org, records, write_precision=WritePrecision.NS)
    a_w = datetime.datetime.now()
    if log:
        print(f"{sensor[0]} | {b_w} | {a_w}")

if __name__ == "__main__":
    print("Running the script")
    asyncio.run(main())