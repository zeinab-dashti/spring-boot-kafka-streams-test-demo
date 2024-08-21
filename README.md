# Testing Strategies for Kafka Streams: From Unit Tests to Integration

## Overview
This repository demonstrate unit and integration tests for two Kafka Streams topologies: [UserTopology](./src/main/java/space/zeinab/demo/streamsTest/service/UserTopology.java) and [UserActivityTopology](./src/main/java/space/zeinab/demo/streamsTest/service/UserActivityTopology.java) . Topologies cover key concepts such as stateless and stateful operations, windowing, and the use of state stores

Unit tests for the topologies are implemented using TopologyTestDriver and integration tests are implemented using EmbeddedKafka and Testcontainers.

## Prerequisites
* Java 17 or higher
* Maven
* Docker (optional, for running Docker Compose which include Zookeeper and Apache Kafka)


## Running the Application
1. **Clone the repository**
   ```sh
   git clone <repository-url>
   cd spring-boot-kafka-streams-test-demo
   ```
   
2. **Run Unit Tests and Integration Test UserTopologyEmbeddedKafkaIT.java**
   ```sh
   mvn clean verify
   ```

3. **Run Integration Test UserTopologyTestContainerIT.java**
   
   Requires Docker to be installed and running and then start Kafka and Zookeeper by using [Docker Compose file](./docker-compose.yml) in the repository:
   ```sh
   docker-compose up
   ```
   When Docker container running Kafka and Zookeeper is spun up then you can run integration test [UserTopologyTestContainersIT](./src/test/java/space/zeinab/demo/streamsTest/service/UserTopologyTestContainersIT.java).
