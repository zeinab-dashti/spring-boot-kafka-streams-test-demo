package space.zeinab.demo.streamsTest.service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import space.zeinab.demo.streamsTest.config.AppConfig;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.equalTo;

@SpringBootTest
public class UserTopologyTestContainersIT extends BaseTest {

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @BeforeEach
    void setUp() {
        KafkaTestContainerSetup.startKafka();
        streamsBuilderFactoryBean.start();
    }

    @AfterEach
    void tearDown() {
        if (streamsBuilderFactoryBean.getKafkaStreams() != null) {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
        KafkaTestContainerSetup.stopKafka();
    }

    @Test
    void buildUserTopology_whenConsumeInputMessages_thenPutItemsInStateStore() {
        produceEvents(AppConfig.INPUT_TOPIC, generateUserObjects(List.of("initial address", "updated address")));

        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() ->
                        getStoreCount().size(), equalTo(1)
                );
    }

    private List<String> getStoreCount() {
        var users = Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType("user-store", QueryableStoreTypes.keyValueStore()))
                .all();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(users, 0), false)
                .map(keyValue -> keyValue.value.toString())
                .toList();
    }
}
