package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataReducer implements Reducer<DoubleWritable, Text, LongWritable, Text> {
    private static final Log log = LogFactory.getLog(MachineDataMapper.class);

    @Override
    public void reduce(DoubleWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {
        while (values.hasNext()) {
            Text value = values.next();
            collector.collect(new LongWritable((long) key.get()), value);
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf conf) {

    }
}
