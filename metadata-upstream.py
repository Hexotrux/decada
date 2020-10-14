import paho.mqtt.client as mqtt
import time
import decada_python_client
import os
import paho.mqtt.publish as publish
import json

Jmessage = ''

def on_connect(client, userdata, flags, rc): #flags is important if not on_connect won't be called
    print("Connection attempt returned: " + mqtt.connack_string(rc))
    print("Successfully connected to MQTT Broker @ 192.168.1.131")

def on_message(client, userdata, message):
    global Jmessage
    topic = str(message.topic)
    #Jmessage = str(message.payload.decode("utf-8"))
    Jmessage = message.payload.decode("utf-8")
    with open("files/metadata.json", "a+") as f:
        f.write(Jmessage + "\n")


ourClient = mqtt.Client() #create MQTT client obj
ourClient.connect("192.168.1.131", 1883) #Connect to MQTT broker
ourClient.on_connect = on_connect
ourClient.subscribe("bvcd/camera/event/obj_in_field") #subscribe to topic
#ourClient.subscribe("bvcd/camera/event/#")
ourClient.on_message = on_message #attach the "messagefunction" to the message event

ourClient.loop_start() #Start the MQTT client

decadaconn = decada_python_client.DecadaPythonClient(os.getcwd(), "/config.yaml") #Connect to DECADA
decadaconn.connect()

while True:
    #with open("files/metadata.json", "r") as f:
     #   Jmessage = f.read()
    if (Jmessage != ''):
        print("Json: ", Jmessage)
        Dmessage = json.loads(Jmessage) #change json to dict

        #Split data into it's respective model elements
        AttrMessage = {
            'ID':Dmessage['ID'],
        }

        MPMessage = {
            'UTC':Dmessage['UTC'],
            'ObjectID':Dmessage['ObjectID'],
            'ObjectClass':Dmessage['ObjectClass'],
            'ruleID': Dmessage['ruleID']
        }

        EventMessage = {
            'eventType':Dmessage['eventType']
        }

        print("Dict mode: ", Dmessage)
        print("Attributes: ", AttrMessage)
        print("Measure Points: ", MPMessage)
        print("Event: ", EventMessage)

        decadaconn.postMeasurePoints(MPMessage) # Post measurepoints to DECADA
        decadaconn.updateAttributes(AttrMessage) # Post attributes to DECADA
        decadaconn.postEvent(EventMessage) # Post events to DECADA

        Jmessage = ''
        time.sleep(5)
    else:
        time.sleep(5)