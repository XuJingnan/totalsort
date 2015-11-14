package com.envision;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataInputFormat extends FileInputFormat<DoubleDescWritable, Text> {

    private JobConf lastConf = null;
    private InputSplit[] lastResult = null;

    class MachineDataRecordReader implements RecordReader<DoubleDescWritable, Text> {
        private LineRecordReader in;
        private LongWritable junk = new LongWritable();
        private Text line = new Text();
        private int startPos;

        public MachineDataRecordReader(Configuration conf, FileSplit split) throws IOException {
            in = new LineRecordReader(conf, split);
            startPos = conf.getInt(TotalOrderSort.CONF_START_POSITION, 1);
        }

        @Override
        public boolean next(DoubleDescWritable key, Text value) throws IOException {
            if (in.next(junk, line)) {
                String tmp = line.toString();
                tmp = tmp.substring(0, tmp.length() - 1);
                key.set(Double.parseDouble(tmp.split(",")[startPos + TotalOrderSort.ADD_POSITION - 1]));
                value.set(tmp);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public DoubleDescWritable createKey() {
            return new DoubleDescWritable();
        }

        @Override
        public Text createValue() {
            return new Text();
        }

        @Override
        public long getPos() throws IOException {
            return in.getPos();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }

        @Override
        public float getProgress() throws IOException {
            return in.getProgress();
        }
    }

    @Override
    public RecordReader<DoubleDescWritable, Text> getRecordReader(InputSplit inputSplit, JobConf conf, Reporter reporter) throws IOException {
        return new MachineDataRecordReader(conf, (FileSplit) inputSplit);
    }

    @Override
    public InputSplit[] getSplits(JobConf conf, int splits) throws IOException {
        if (conf == lastConf) {
            return lastResult;
        }
        lastConf = conf;
        lastResult = super.getSplits(conf, splits);
        return lastResult;
    }
}

