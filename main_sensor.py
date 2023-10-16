from pymodbus.client.sync import ModbusTcpClient as ModbusClient
import time
import datetime 
import threading
import paho.mqtt.client as mqtt
import pickle
import json
import asyncio
# import absl.logging
# absl.logging.set_verbosity(absl.logging.ERROR)
# import os

with open('backup.pickle', 'rb') as f:
    backup = pickle.load(f)
if backup != 0:
    total = backup
    print(total)
else:
    total = 0
timestart = time.time()
irra = 0
nhietdo = 0
status = None

def reset_total():
    timereset = int(time.mktime(datetime.datetime(2023, 9, 18, 17, 0 ).timetuple()))
    global total
    while True:
        if time.time() >= timereset:
            total = 0
            with open('backup.pickle', 'wb') as f:
                pickle.dump(total, f)
            timereset = timereset + 86400
        time.sleep(1)


def read_data():
    time.sleep(3)
    global total,timestart, irra, nhietdo , status
    client1 = ModbusClient('192.168.0.107', port= 502, timeout=1)

    if client1.connect():
            print("connected device")
            while True:
                
                try:  
                    read = client1.read_input_registers(address=6, count=3, unit=16)
                    nhietdo =  round(int(read.registers[2])/10,1)
                    # a = str(format(read.registers[0], '016b')) + str(format(read.registers[1], '016b'))
                    # if a[0] == '1':
                    #     irra = 0
                    # else:
                    #     irra = int(a,2)/100
                    irra = read.registers[0]
                    time.sleep(0.2)
                    total = total + (irra*(time.time()-timestart))/3600
                    timestart = time.time()
                    status = 1
                    with open('backup.pickle', 'wb') as f:
                        pickle.dump(total, f)
                    # timestart = time.time()
                    # print(irra)
                except:
                    print("Device lost connection")
                    status = 0
                    restart()     
                    break              
    
    else:
        # print("Connect failed")
        status = 0
        time.sleep(5)
        print("Connect to the device failed!!!   Try to connect to the device...")
        restart()

def send_data():
    time.sleep(3)
    mqtt_broker = "core.ziot.vn"
    mqtt_port = 5000
    mqtt_topic = "DEVICE/DO000000/PO000008/SI000009/PL000012/DE000125/reportData"
    client = mqtt.Client()
    client.username_pw_set('iot2022', 'iot2022')
    try:
        client.connect(mqtt_broker, mqtt_port)
        print("MQTT connected server.")
        client.disconnect()
        sts = 1
    except:
        sts = 0
    if sts == 1:
        while True:

            try:   
                a = 300 * (time.time() // 300) + 300            
                time.sleep(a - time.time())
                timeStamp = datetime.datetime.fromtimestamp(a)
                # print(a)

                if  status == 1:
                    irr = irra
                    client.connect(mqtt_broker, mqtt_port)
                    # print("MQTT connected server.")
                    DATA = {"type": "smp3", "data": [{"totalIrradce": round(irr)}, {"dailyIrradtn": round(total/1000,3)}, {"ambientTemp": nhietdo}], "timeStamp": str(timeStamp)}
                    data = json.dumps(DATA)
                    client.publish(mqtt_topic, data)
                    time.sleep(10)
                    print(data)
                    client.disconnect()
                    
                elif status == 0 :
                    print("Device lost connection \nMQTT temporarily does not send data")
                    mqtt_reconnect()    
                    break

            except:
                print("MQTT lost connect")
                time.sleep(5)
                print("Try to connect with MQTT server...")
                mqtt_reconnect()
                break

    else:
        # print("MQTT connect failed")
        time.sleep(5)
        print("MQTT connect failed!!!   Try to connect with MQTT server...")
        mqtt_reconnect() 
        


def run_main():   
    threading.Thread(target=reset_total).start()    
    threading.Thread(target=send_data).start()
    threading.Thread(target=read_data).start()

def restart():
    time.sleep(2)
    threading.Thread(target=read_data).start()  
    # threading.Thread(target=read_data).join()

def mqtt_reconnect():
    time.sleep(2)
    threading.Thread(target=send_data).start()    
    
    # threading.Thread(target=send_data).join()
    
run_main()



