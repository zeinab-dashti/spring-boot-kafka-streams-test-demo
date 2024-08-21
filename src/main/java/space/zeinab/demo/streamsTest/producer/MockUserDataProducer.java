package space.zeinab.demo.streamsTest.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import space.zeinab.demo.streamsTest.model.User;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static space.zeinab.demo.streamsTest.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class MockUserDataProducer {
    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        produceUsersData(objectMapper);
    }

    private static void produceUsersData(ObjectMapper objectMapper) {
        String[] userIds = {"user1"/*, "user2", "user3"*/};
        List<User> users = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            for (String userId : userIds) {
                //data to simulate exception
                //User data = new User(userId, userId + "-name", userId + "-Invalid address", LocalDateTime.now());
                User data = new User(userId, userId + "-name", userId + "-address", LocalDateTime.now());
                users.add(data);
            }
        }

        users.forEach(user -> {
            try {
                String userJSON = objectMapper.writeValueAsString(user);
                RecordMetadata recordMetaData = publishMessageSync(user.userId(), userJSON);
                log.info("Published the user data : {} ", recordMetaData);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        });
    }
}