package bloody.reprocessing;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author <a href="mailto:melnikyura.dev@gmail.com">Yuriy Melnik</a>
 * Created on July 31, 2022
 */
@Slf4j
public class RetryConsumerRebalanceListener implements org.apache.kafka.clients.consumer.ConsumerRebalanceListener {
    String retryRegex;
    String retryTopic;
    String dlqTopic;
    Set<String> mainTopicsMasterData = new HashSet<>();
    Map<String, Long> retryTopicsMasterData = new HashMap<>();

    public RetryConsumerRebalanceListener(String retryRegex, String dlqTopic) {
        this.retryRegex = retryRegex;
        this.dlqTopic = dlqTopic;
        retryTopic = retryRegex.replaceAll("\\\\\\.", ".").replaceAll("\\\\d\\+", "");
    }

    @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.info("Revoked partitions: [{}]", partitions);
    }

    @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        log.info("Assigned partitions: [{}]", partitions);
        for (TopicPartition partition : partitions) {
            String topic = partition.topic();
            if (topic.matches(retryRegex)) {
                retryTopicsMasterData.putIfAbsent(topic, retrieveTimeoutFromTopic(topic));
            } else {
                mainTopicsMasterData.add(topic);
            }
        }
    }

    public boolean isMainTopic(String topic) {
        return mainTopicsMasterData.contains(topic);
    }

    /**
     * Returns timeout of retry topic based on topic name
     * @param topic topic name
     * @return timeout in milliseconds, or 0 if provided is not retry topic
     */
    public long getRetryTopicTimeoutMilli(String topic) {
        return retryTopicsMasterData.getOrDefault(topic, 0L) * 1000;
    }

    /**
     * Возвращает следующие по очереди retry-топик, в который должно быть отправлено сообщение, если при обработке сообщения
     * из указанного <code>currentTopic</code> произошло сообщение
     * @param currentTopic Текущий топик, при обработке сообщения из которого произошла ошибка
     * @return Следующий retry-топик или DLQ-топик, если текущий топик последний в очереди retry
     */
    public String getNextTopic(String currentTopic) {
        Long currentTimeout = getRetryTopicTimeoutMilli(currentTopic)/1000;
        Collection<Long> supportedTimeouts = retryTopicsMasterData.values();
        Optional<Long> nextTimeout = supportedTimeouts.stream().sorted().filter(aLong -> aLong > currentTimeout).findFirst();

        return nextTimeout.map(aLong -> retryTopic + aLong).orElseGet(() -> dlqTopic);
    }


    /**
     * Получает значение таймаута retry-топика, из его имени
     * @param topic Наименование топика
     * @return Таймаут - если заданный топик является retry-топиком, иначе - 0
     */
    private long retrieveTimeoutFromTopic(String topic) {
        return topic.matches(retryRegex) ? Long.parseLong(topic.split("_retry")[1]) : 0L;
    }
}
