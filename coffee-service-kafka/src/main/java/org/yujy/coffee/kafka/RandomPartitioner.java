package org.yujy.coffee.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

import java.util.Random;

/**
 * Random分片实现.
 */
public class RandomPartitioner implements Partitioner {

    public RandomPartitioner(VerifiableProperties verifiableProperties) { }

    public int partition(Object key, int numPartitions) {
        Random random = new Random();
        return random.nextInt(numPartitions);
    }
}
