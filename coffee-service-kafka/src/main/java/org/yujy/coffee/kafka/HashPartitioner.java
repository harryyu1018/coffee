package org.yujy.coffee.kafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * Hash 自定义分片.
 */
public class HashPartitioner implements Partitioner {

    public HashPartitioner(VerifiableProperties verifiableProperties) { }

    public int partition(Object key, int numPartitions) {

        System.out.println(String.format("==== the instance of partition is: %s, the key: %s", this, key));

        if (key instanceof Integer) {
            return Math.abs(Integer.parseInt(key.toString())) % numPartitions;
        }

        return Math.abs(key.hashCode() % numPartitions);
    }
}
