package space.zeinab.demo.streamsTest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.stereotype.Component;
import space.zeinab.demo.streamsTest.model.User;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@Component
public class BaseTest {
    Consumer<String, User> consumer;
    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired
    private ObjectMapper objectMapper;

    List<User> generateUserObjects(List<String> addresses) {
        return addresses.stream().map(address -> new User("user", "user-name", address, LocalDateTime.now())).toList();
    }

    void produceEvents(String inputTopicName, List<User> users) {
        users.forEach(event -> {
            String eventJSON;
            try {
                eventJSON = objectMapper.writeValueAsString(event);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            kafkaTemplate.send(inputTopicName, event.userId(), eventJSON);
        });
    }

    Consumer<String, User> configureConsumer(String broker, String outputTopicName) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.springframework.kafka.support.serializer.JsonDeserializer");
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "space.zeinab.demo.streamsTest.model.User");
        consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(outputTopicName));

        return consumer;
    }

    ConsumerRecord<String, User> consumeRecord(String broker, String outputTopicName) {
        Consumer<String, User> consumer = configureConsumer(broker, outputTopicName);
        return KafkaTestUtils.getSingleRecord(consumer, outputTopicName);
    }

    void stopConsumer() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
