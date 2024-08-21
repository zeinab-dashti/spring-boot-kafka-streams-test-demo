package space.zeinab.demo.streamsTest.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import space.zeinab.demo.streamsTest.model.User;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Slf4j
public class UserActivityTimeStampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> consumerRecord, long partitionTime) {
        User user = (User) consumerRecord.value();
        if (user != null && user.modifiedTime() != null) {
            LocalDateTime timeStamp = user.modifiedTime();
            log.info("timeStamp in extractor : {} ", timeStamp);

            return timeStamp.toInstant(ZoneOffset.UTC).toEpochMilli();
        }
        return partitionTime;
    }
}