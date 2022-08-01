package bloody.reprocessing;

import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author <a href="mailto:melnikyura.dev@gmail.com">Yuriy Melnik</a>
 * Created on July 31, 2022
 */
@Slf4j
public class Application {
    private static final String RETRY_TOPICS_REGEX = "retry.topic.regex";
    private static final String MAIN_TOPIC_REGEX = "main.topic.regex";
    private static final String DLQ_TOPIC = "dlq.topic";

    private static final Map<TopicPartition, Instant> pausedPartitions = new HashMap<>();

    public static void main(String[] args) throws IOException {
        final Properties appConfig = new Properties();
        InputStream in = Application.class.getClassLoader().getResourceAsStream("application.properties");
        appConfig.load(in);
        assert in != null;
        in.close();

        final Properties consumerConfig = new Properties();
        in = Application.class.getClassLoader().getResourceAsStream("kafkaconsumer.properties");
        consumerConfig.load(in);
        assert in != null;
        in.close();

        final Properties producerConfig = new Properties();
        in = Application.class.getClassLoader().getResourceAsStream("kafkaproducer.properties");
        producerConfig.load(in);
        assert in != null;
        in.close();

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
             final KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig)) {

            final RetryConsumerRebalanceListener retryConsumerListener = new RetryConsumerRebalanceListener(
                    appConfig.getProperty(RETRY_TOPICS_REGEX),
                    appConfig.getProperty(DLQ_TOPIC)
            );
            consumer.subscribe(
                    Pattern.compile(appConfig.getProperty(RETRY_TOPICS_REGEX)+"|"+appConfig.getProperty(MAIN_TOPIC_REGEX)),
                    retryConsumerListener
            );

            /* Партиции, которые нужно поставить на паузу в начинающейся итерации */
            Set<TopicPartition> toPause = new HashSet<>();

            while (true) {
                Instant now = Instant.now();

                /* Poll */
                if (!toPause.isEmpty()) {
                    log.info("Set on pause: [{}]", toPause);
                    try {
                        consumer.pause(toPause);
                        toPause.clear();
                    } catch (IllegalStateException e) {
                        log.debug("Scheduled for pause partition is not assigned to this consumer any more", e);
                    }
                }
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    if (!retryConsumerListener.isMainTopic(record.topic())) {
                        /* Сообщение получено из retry-топика */
                        TopicPartition recordPartition = new TopicPartition(record.topic(), record.partition());

                        if (toPause.contains(recordPartition)) {
                            log.info("Message is from paused topic, so ignoring it: [{}]", record);
                            continue;
                        }

                        if (now.toEpochMilli() < record.timestamp() + retryConsumerListener.getRetryTopicTimeoutMilli(record.topic())) {
                            /*
                              Время ожидания в retry-топике ещё не истекло, поэтому эту партицию нужно поставить на паузу:
                              recordPartition -> toPause // Добавляем партицию в список тех, которые нужно поставить на паузу в следующей итерации
                              recordPartition & resumeTime -> schedule // Добавляем партицию со временем возобновления в рассписание
                             */
                            toPause.add(recordPartition);
                            addPartitionToSchedule(recordPartition, Instant.ofEpochMilli(record.timestamp() + retryConsumerListener.getRetryTopicTimeoutMilli(record.topic())));
                            long recordOffset = record.offset();
                            consumer.seek(recordPartition, recordOffset);

                            continue;
                        }
                    }

                    try {
                        doLogic(record);

                    } catch (Exception e) {
                        String topic = retryConsumerListener.getNextTopic(record.topic());
                        producer.send(new ProducerRecord<>(topic, record.key(), record.value()));
                        log.info("Send to retry: [{}]", topic);
                    }
                }

                final Set<TopicPartition> toResume = getPartitionsScheduledBefore(now);
                if (!toResume.isEmpty()) {
                    try {
                        consumer.resume(toResume);
                    } catch (IllegalStateException e) {
                        log.debug("Prepared for resume partition is not assigned to this consumer any more", e);
                    }
                    log.info("Resumed: [{}]", toResume);
                }
            }
        }
    }

    private static void doLogic(ConsumerRecord<String, String> record) throws Exception {
        log.info("Performing with: {}-{}, offset: {}, time: {}", record.topic(), record.partition(), record.offset(), Instant.ofEpochMilli(record.timestamp()));
        HttpResponse<JsonNode> response = Unirest.get("https://db12d221-29df-45d6-89fb-2ea065a3cdec.mock.pstmn.io/account/{id}/balance")
                .routeParam("id", record.value())
                .asJson();
        log.info("Response: [{}: {}]", response.getStatusText(), response.getBody());
        if (!response.isSuccess()) {
            throw new Exception(response.getStatusText());
        }
    }

    private static void addPartitionToSchedule(TopicPartition partition, Instant time) {
        pausedPartitions.putIfAbsent(partition, time);
    }

    private static Set<TopicPartition> getPartitionsScheduledBefore(Instant time) {
        final Set<TopicPartition> partitions = new HashSet<>();

        for (TopicPartition partition : pausedPartitions.keySet()) {
            if (pausedPartitions.get(partition).compareTo(time) < 0)
                partitions.add(partition);
        }

        for (TopicPartition partition : partitions) {
            pausedPartitions.remove(partition);
        }

        return partitions;
    }
}
