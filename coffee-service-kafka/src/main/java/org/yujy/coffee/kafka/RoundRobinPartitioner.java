package org.yujy.coffee.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Round Robin自定义分片.
 */
public class RoundRobinPartitioner implements Partitioner {

    private static AtomicLong next = new AtomicLong();

    public RoundRobinPartitioner(VerifiableProperties verifiableProperties) { }

    public int partition(Object key, int numPartitions) {
        long nextIndex = next.incrementAndGet();
        return (int) nextIndex % numPartitions;
    }
}
