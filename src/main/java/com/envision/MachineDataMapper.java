package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.IOException;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataMapper extends IdentityMapper<DoubleWritable, Text> {
    private static final Log log = LogFactory.getLog(MachineDataMapper.class);

    @Override
    public void map(DoubleWritable key, Text val, OutputCollector<DoubleWritable, Text> output, Reporter reporter) throws IOException {
        super.map(key, val, output, reporter);
    }
}
