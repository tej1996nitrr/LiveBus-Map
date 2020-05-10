from pykafka import KafkaClient
client = KafkaClient(hosts="localhost:9092")
# print(client.topics)
# print(client.topics['test_topic'])
topic = client.topics['testBusData']
producer = topic.get_sync_producer()
producer.produce('test message'.encode('ascii'))
count=1
while True:
    message = ("hello"+str(count)).encode('ascii')
    producer.produce(message)
    print(message)
    count+=1