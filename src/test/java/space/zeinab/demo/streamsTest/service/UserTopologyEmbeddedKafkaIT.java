package space.zeinab.demo.streamsTest.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.model.User;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Spliterators;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.equalTo;

@SpringBootTest
@EmbeddedKafka
@TestPropertySource(properties = {
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class UserTopologyEmbeddedKafkaIT extends BaseTest {

    @Value("${spring.embedded.kafka.brokers}")
    private String broker;

    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    @BeforeEach
    void setUp() {
        streamsBuilderFactoryBean.start();
    }

    @AfterEach
    void tearDown() {
        if (streamsBuilderFactoryBean.getKafkaStreams() != null) {
            KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
            //streamsBuilderFactoryBean.stop();
            kafkaStreams.close();
            kafkaStreams.cleanUp();
        }
        stopConsumer();
    }

    @Test
    void userTopology_whenValidInputMessages_thenProduceCorrectOutputMessage() {
        produceEvents(AppConfig.INPUT_TOPIC, generateUserObjects(List.of("initial address", "updated address")));
        ConsumerRecord<String, User> record = consumeRecord(broker, AppConfig.OUTPUT_TOPIC);

        assertThat(record).isNotNull();
        assertThat(record.value().address()).contains("updated address");
    }

    @Test
    void getStateStoreItems_whenValidInput_thenCorrectItemCount() {
        produceEvents(AppConfig.INPUT_TOPIC, generateUserObjects(List.of("initial address", "updated address")));

        Awaitility.await().atMost(20, TimeUnit.SECONDS)
                .pollDelay(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() ->
                        getStoreCount().size(), equalTo(1)
                );
    }

    private List<String> getStoreCount() {
        var outputUsers = Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
                .store(StoreQueryParameters.fromNameAndType("user-store", QueryableStoreTypes.keyValueStore()))
                .all();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(outputUsers, 0), false)
                .map(keyValue -> keyValue.value.toString())
                .toList();
    }
}
