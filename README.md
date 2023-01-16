# Kafka - Cassandra
O objetivo desse projeto é estudar a integração do Kafka com Pyspark e Cassandra. Os dados são gerados pelo `producer.py` usando a lib `Faker` e enviados para o `consumer.py` pelo Kafka. Após o filtro do Pyspark, os dados entregues pelo Kafka são inseridos em outro tópico do mesmo e inseridos em um banco de dados do Cassandra.
