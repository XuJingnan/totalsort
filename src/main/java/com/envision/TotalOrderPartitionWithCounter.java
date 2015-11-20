package com.envision;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

/**
 * Created by xujingnan on 15-11-14.
 */
public class TotalOrderPartitionWithCounter extends TotalOrderPartitioner<DoubleDescWritable, Text> {

    @Override
    public int getPartition(DoubleDescWritable key, Text value, int numPartitions) {
        int part = super.getPartition(key, value, numPartitions);
        Tools.increseRecord(part);
        return part;
    }
}
