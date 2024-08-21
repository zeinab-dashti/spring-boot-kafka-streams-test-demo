package space.zeinab.demo.streamsTest.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import space.zeinab.demo.streamsTest.config.AppConfig;

import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerUtil {
    static KafkaProducer<String, String> producer = new KafkaProducer<String, String>(AppConfig.getProducerProperties());

    public static RecordMetadata publishMessageSync(String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(AppConfig.INPUT_TOPIC, key, message);
        RecordMetadata recordMetadata = null;

        try {
            log.info("producerRecord : " + producerRecord);
            recordMetadata = producer.send(producerRecord).get();
        } catch (InterruptedException e) {
            log.error("InterruptedException in  publishMessageSync : {}  ", e.getMessage(), e);
        } catch (ExecutionException e) {
            log.error("ExecutionException in  publishMessageSync : {}  ", e.getMessage(), e);
        } catch (Exception e) {
            log.error("Exception in  publishMessageSync : {}  ", e.getMessage(), e);
        }
        return recordMetadata;
    }
}