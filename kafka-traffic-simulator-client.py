"""
Created on Sun Mar 26 12:03:58 2023

@author: David Hurtgen

Program to create a Kafka client and topic for traffic simulation
"""

from kafka.admin import KafkaAdminClient, NewTopic

# admin client and topic
admin_client = KafkaAdminClient(bootstrap_servers="localhost:9092", client_id="python_ts_client")

topic_list = []
topic = NewTopic(name="traffic-simulator", num_partitions=10, replication_factor=1)
topic_list.append(topic)
admin_client.create_topics(new_topics=topic_list)

# End of program