import json
import time
from kafka import KafkaProducer
from faker import Faker
import random as rd

faker_user = Faker()

def serializer(data):
    return json.dumps(data).encode('utf-8')

kafka_producer = KafkaProducer(
    bootstrap_servers = ['127.0.0.1:9092'], 
    value_serializer = serializer
)

def generate_data():
    user_name = faker_user.name()
    user_location = faker_user.city()
    user_email = faker_user.email()
    user_job = faker_user.job()
    user_spent = rd.randrange(0, 100000)
    user_country = faker_user.country()
    user_id = str(rd.randrange(0,99999999999999999999))
    user = {
        'user_id': user_id,
        'name': user_name,
        'email': user_email,
        'location': user_location,
        'country': user_country,
        'job': user_job,
        'spent': user_spent
    }
    return user

if __name__ == '__main__':
    for count in range(10):
        data = generate_data()
        print(data)
        kafka_producer.send('source_userdata', data)
        time.sleep(1)
          