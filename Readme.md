`kafka-topics.sh --create --topic streams-app-wordcount-output --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`

`kafka-topics.sh --create --topic streams-app-wordcount-input --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1`