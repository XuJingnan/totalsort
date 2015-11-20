package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by xujingnan on 11/19/15.
 */
public class MachineDataInputFormat extends FileInputFormat<DoubleDescWritable, Text> {
    public static final Log log = LogFactory.getLog(MachineDataInputFormat.class);

    private JobContext context = null;
    private List<InputSplit> lastResult = null;

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        if (this.context == context) {
            return lastResult;
        }
        this.context = context;
        this.lastResult = super.getSplits(context);
        return lastResult;
    }

    @Override
    public RecordReader<DoubleDescWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new MachineDataRecordReader();
    }

    public static class MachineDataRecordReader extends RecordReader<DoubleDescWritable, Text> {
        private LineRecordReader inReader;
        private Text originalValue = new Text();
        private DoubleDescWritable key = new DoubleDescWritable();
        private Text value = new Text();
        private int startPos;
        public String seperator;

        public MachineDataRecordReader() {
            inReader = new LineRecordReader();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            inReader.initialize(split, context);
            startPos = Tools.getInt(context, Tools.CONF_START_POSITION, 1);
            int flag = Tools.getInt(context, Tools.CONF_SEPERATOR_FLAG);
            switch (flag) {
                case 0:
                    seperator = ",";
                    break;
                case 1:
                    seperator = "\001";
                    break;
                default:
                    seperator = "\t";
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (inReader.nextKeyValue()) {
                originalValue = inReader.getCurrentValue();
                key.set(Double.parseDouble(originalValue.toString().split(seperator)[startPos + Tools.ADD_POSITION - 1]));
                value.set(originalValue);
                return true;
            } else {
                return false;
            }
        }

        @Override
        public DoubleDescWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return inReader.getProgress();
        }

        @Override
        public void close() throws IOException {
            inReader.close();
        }

        public Class getKeyClass() {
            return DoubleDescWritable.class;
        }
    }
}
