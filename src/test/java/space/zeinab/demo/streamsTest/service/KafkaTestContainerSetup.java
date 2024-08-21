package space.zeinab.demo.streamsTest.service;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTestContainerSetup {
    private static final KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"));

    public static void startKafka() {
        kafkaContainer.start();
    }

    public static void stopKafka() {
        kafkaContainer.stop();
    }

    public static String getBootstrapServers() {
        return kafkaContainer.getBootstrapServers();
    }
}