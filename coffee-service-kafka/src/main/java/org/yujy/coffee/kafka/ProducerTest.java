package org.yujy.coffee.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

import java.util.Properties;

/**
 * kafka生产者实践Demo.
 */
public class ProducerTest {

    private static final String TOPIC = "topic1";
    private static final String ZOOKEEPER = "localhost:2181";
    private static final String BROKER_LIST = "localhost:9092";
    private static final int PARTITIONS = 3;

    private static Producer<String, String> initProducer() {

        Properties props = new Properties();
        props.put("metadata.broker.list", BROKER_LIST);
        props.put("serializer.class", StringEncoder.class.getName());
        props.put("partitioner.class", HashPartitioner.class.getName());
        props.put("producer.type", "sync");

        props.put("batch.num.messages", "3");
        props.put("queue.buffer.max.ms", "10000000");
        props.put("queue.buffering.max.messages", "1000000");
        props.put("queue.enqueue.timeout.ms", "20000000");

        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);

        return producer;
    }

    public static void sendOne(Producer<String, String> producer, String topic) {

        String[][] messages = new String[][] {
                {"21", "test 11"},
                {"22", "test 12"},
                {"23", "test 13"}
        };

        for (String[] msg : messages) {
            KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, msg[0], msg[1]);
            producer.send(keyedMessage);
        }
    }

    public static void main(String[] args) {

        Producer<String, String> producer = initProducer();
        sendOne(producer, TOPIC);
    }

}
