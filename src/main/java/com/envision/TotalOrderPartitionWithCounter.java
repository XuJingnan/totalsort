package com.envision;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

/**
 * Created by xujingnan on 15-11-14.
 */
public class TotalOrderPartitionWithCounter extends TotalOrderPartitioner<DoubleDescWritable, Text> {

    @Override
    public int getPartition(DoubleDescWritable key, Text value, int numPartitions) {
        int part = super.getPartition(key, value, numPartitions);
        MachineDataMapper.reduceInputRecords.set(part, MachineDataMapper.reduceInputRecords.get(part) + 1);
        return part;
    }
}
