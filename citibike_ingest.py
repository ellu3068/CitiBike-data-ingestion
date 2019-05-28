# kafkacat use "python filename" to run the file

import ujson
import json
import time
import requests
from retrying import retry
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer

admin = AdminClient({'bootstrap.servers': 'kafka-1:9092'})
admin.create_topics([NewTopic("citibike.station.update.1", num_partitions=3, replication_factor=1)])
p = Producer({'bootstrap.servers': 'kafka-1:9092', 'api.version.request': True})

# retry decorator default: retry forever without waiting (@retry)
@retry(wait_fixed = 10000)
def scrape_station_status():
	r = requests.get("https://gbfs.citibikenyc.com/gbfs/es/station_status.json")
	if r.status_code != 200:
		print("try again!")
		raise IOError("Station status fetch failed!")
	return r.json()


while True:
	data = scrape_station_status()
	outputs = data['data']['stations']

	for entry in outputs:
		print(entry)

		bytes_object = json.dumps(entry).encode('utf-8')
		#name = 'citibike.station.update.' + str(r)

		p.produce("citibike.station.update.1", bytes_object)

	#p.flush()
	print("process completed!")

	time.sleep(10)
