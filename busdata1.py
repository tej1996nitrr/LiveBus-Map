
from pykafka import KafkaClient
import json
import uuid
from datetime import datetime

client = KafkaClient(hosts="localhost:9092")
topic = client.topics['testBusData']
producer = topic.get_sync_producer()


with open('data/bus1.json') as f:
  data = json.load(f)
  coordinates = data['features'][0]['geometry']['coordinates']

def generate_uuid():
    return uuid.uuid4()

data={}
data['busline']='0001'
data['key'] = data['busline']+str(generate_uuid())
data['timestamp'] = str(datetime.utcnow())
data['latitude'] = coordinates[0][1]
data['longitude'] = coordinates[0][0]

def generate_checkpoint(coordinates):
    i = 0
    while i <len(coordinates):
        data['key'] =  data['busline'] +'_'+str(generate_uuid())
        data['timestamp'] = str(datetime.utcnow())
        data['latitude'] = coordinates[i][1]
        data['longitude'] = coordinates[i][0]
        message= json.dumps(data)
        print(message)
        producer.produce(message.encode('ascii'))

        # if bus  reaches a  last coordinate, start from beginning
        if i==len(coordinates)-1:
            i=0
        i+=1
generate_checkpoint(coordinates)


print(data) 



