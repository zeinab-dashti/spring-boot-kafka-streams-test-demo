package space.zeinab.demo.streamsTest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;
import space.zeinab.demo.streamsTest.config.AppConfig;
import space.zeinab.demo.streamsTest.model.User;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserTopology {
    private final ObjectMapper objectMapper;

    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {
        KStream<String, User> inputUser = streamsBuilder.stream(
                AppConfig.INPUT_TOPIC, Consumed.with(Serdes.String(), new JsonSerde<>(User.class, objectMapper))
        );
        inputUser.print(Printed.<String, User>toSysOut().withLabel("Input stream for user topology"));

        KStream<String, User> modifiedUser = inputUser
                .mapValues(value -> new User(value.userId(), value.name().toUpperCase(), value.address(), LocalDateTime.now()))
                .mapValues(value -> {
                    if (value.address().contains("Invalid address")) {
                        try {
                            throw new IllegalStateException(value.address());
                        } catch (Exception e) {
                            log.error("Exception is userTopology : {} ", value);
                            return null;
                        }
                    }
                    return value;
                }).filter((key, value) -> key != null && value != null);

        KTable<String, User> reducedUser = modifiedUser
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(User.class, objectMapper)))
                .reduce(
                        (oldValue, newValue) -> newValue,
                        Materialized.<String, User, KeyValueStore<Bytes, byte[]>>as("user-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(User.class, objectMapper))
                );
        reducedUser.toStream().print(Printed.<String, User>toSysOut().withLabel("Reduced stream"));

        reducedUser.toStream().to(AppConfig.OUTPUT_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(User.class, objectMapper)));
    }
}