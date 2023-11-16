docker run -d -p 9092:9092 \
            -e ALLOW_ANONYMOUS_LOGIN=yes \
            -e KAFKA_ADVERTISED_HOST_NAME=localhost \
            -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
            -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
            -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092 \
            -e KAFKA_CREATE_TOPICS=test:1:1 \
            -e ALLOW_PLAINTEXT_LISTENER=yes \
            bitnami/kafka:latest