package org.yujy.coffee.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Consumer Test示例.
 */
public class ConsumerTest {

    public static void main(String[] args) {

        String zk = "localhost:2181";
        String topic = "topic1";
        String groupId = "group1";
        String consumerId = "consumer1";

        Properties props = new Properties();
        props.put("zookeeper.connect", zk);
        props.put("group.id", groupId);
        props.put("autooffset.reset", "largest");
        props.put("autocommit.enable", "true");
        props.put("client.id", "test");
        props.put("auto.commit.interval.ms", "1000");

        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream1 = consumerMap.get(topic).get(0);

        ConsumerIterator<byte[], byte[]> it1 = stream1.iterator();
        while (it1.hasNext()) {
            MessageAndMetadata<byte[], byte[]> messageAndMetadata = it1.next();
            String message =
                    String.format("Consumer ID:%s, Topic:%s, GroupID:%s, PartitionID:%s, Offset:%s, Message Key:%s, Message Payload: %s",
                            consumerId,
                            messageAndMetadata.topic(), groupId, messageAndMetadata.partition(),
                            messageAndMetadata.offset(), new String(messageAndMetadata.key()), new String(messageAndMetadata.message()));
            System.out.println(message);
        }

    }
}
