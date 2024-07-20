import os
import datetime
import random
import time
import influxdb_client
from influxdb_client import InfluxDBClient, Point, WritePrecision, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS
 
def random_hex_string(length=4):
    return os.urandom(length).hex()
 
 
def generate_measurement(name, timestamp, length):
    #print(name, timestamp, length)
    ret_dict = {"measurement": name,
                "time": timestamp}
    gen_fields = {}
    for value in range(length):
        gen_fields["f-" + str(value)] = random.uniform(0.1, 100.0)
    ret_dict["fields"] = gen_fields
        #ret_dict["field-" + str(value)] = random.uniform(0.1, 100.0)
    return ret_dict
 
def get_pump_count():
    env = 'PUMPCOUNT'
    pump_count = 20
    if env in os.environ:
        pump_count_str = os.environ[env]
        if pump_count_str.isnumeric:
            pump_count = int(pump_count_str)
        if pump_count == 0:
            pump_count = 1
    return pump_count
 
def generate_pump(number):
    lauf = "-LC" + str(number)
    # generate DP 7x1, 3x30, 2x5
    sensors = []
    for value in range(7):
        sensors.append([random_hex_string() + lauf, 1])
    for value in range(3):
        sensors.append([random_hex_string() + lauf, 30])
    for value in range(2):
        sensors.append([random_hex_string() + lauf, 5])
    return sensors
 
def generate_pump_list(pump_count):
    pumps = []
    i = 0
    while i < pump_count:
        pumps.append([generate_pump(i), []])
        i = i+1
    return pumps
 
def print_log(output, text):
    if output:
        print(text)
#main
 
 
 
 
 
#print(random_hex_string())
 
 
 
 
 
 
#print(measurements)
 
#InfluxDB
bucket = os.environ['BUCKET']
org = os.environ['ORG']
token = os.environ['TOKEN']
url = os.environ['URL']
 
 
client = influxdb_client.InfluxDBClient(
    url=url,
    token=token,
    org=org
)
 
write_api = client.write_api(write_options=SYNCHRONOUS)
 
log = False
 
#Schleife für Anzahl der Pumpen
while True:
    pump_count = get_pump_count()
    print_log(log,"Pump_count = " + str(pump_count))
 
    # pumpen erszeugen
    print_log(log,"Start Pump generation")
    pump_list = generate_pump_list(pump_count)
    print_log(log,"End Pump generation")
 
 
 
    sekunde = 0
    while sekunde < 60:
        #jede Sekunde für jede Pumpe Messwerte erzeugen
        dt = datetime.datetime.now()
        #print(dt)
        pump_values = []
        for pump in pump_list:
            records = []
            for sensor in pump[0]:
                    #print(x)
                    records.append(generate_measurement(sensor[0], dt, sensor[1]))
            pump_values.append(records)
            pump[1].append(pump_values)
        sekunde = sekunde + 1
        time.sleep(1)
 
 
 
    #Jede minute für alle Pumpen die Datensätze schreiben
    for values in pump_list:
        b_w = datetime.datetime.now()
        write_api.write(bucket, org, values[1], write_precision="ns")
        a_w =datetime.datetime.now()
        print(values[0][0][0] + "|" + str(b_w) + "|" + str(a_w))
 
 
 
"""
 
        #Schleide zum Schreiben
        while True:
            records = []
            for value in range(60):
                dt = datetime.datetime.now()
                print(str(dt) + " - " +  str(value))
               
                for sensor in pump:
                    #print(x)
                    records.append(generate_measurement(sensor[0], dt, sensor[1]))
 
                #print("--------------------------------------")
                #for y in records:
                #   print(y)
                #   print("")
                time.sleep(1)
 
            write_api.write(bucket, org, records, write_precision="ns")
"""   