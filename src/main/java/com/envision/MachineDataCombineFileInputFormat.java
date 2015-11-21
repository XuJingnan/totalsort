package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by xujingnan on 11/21/15.
 */
public class MachineDataCombineFileInputFormat extends CombineFileInputFormat<DoubleDescWritable, Text> {
    public static final Log log = LogFactory.getLog(MachineDataCombineFileInputFormat.class);
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
    public RecordReader<DoubleDescWritable, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader<DoubleDescWritable, Text>(
                (CombineFileSplit) split, context, MachineDataCombineFileLineRecordReader.class);
    }

    public static class MachineDataCombineFileLineRecordReader extends RecordReader<DoubleDescWritable, Text> {
        private int startPos;
        public String seperator;

        private FileSystem fs;
        private Path path;
        private long startOffset; //offset of the chunk;
        private long end; //end of the chunk;
        private FSDataInputStream fileIn;
        private LineReader reader;

        private long pos; // current pos
        private DoubleDescWritable key;
        private Text value;

        public MachineDataCombineFileLineRecordReader(
                CombineFileSplit split, TaskAttemptContext context, Integer index) throws IOException {
            this.path = split.getPath(index);
            fs = this.path.getFileSystem(context.getConfiguration());
            this.startOffset = split.getOffset(index);
            this.end = startOffset + split.getLength(index);
            boolean skipFirstLine = false;

            //open the file
            fileIn = fs.open(path);
            if (startOffset != 0) {
                skipFirstLine = true;
                --startOffset;
                fileIn.seek(startOffset);
            }
            reader = new LineReader(fileIn);
            if (skipFirstLine) {  // skip first line and re-establish "startOffset".
                startOffset += reader.readLine(new Text(), 0,
                        (int) Math.min((long) Integer.MAX_VALUE, end - startOffset));
            }
            this.pos = startOffset;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            key = new DoubleDescWritable();
            value = new Text();
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
            int newSize = 0;
            if (pos < end) {
                newSize = reader.readLine(value);
                key.set(Double.parseDouble(value.toString().split(seperator)[startPos + Tools.ADD_POSITION - 1]));
                pos += newSize;
            }
            if (newSize == 0) {
                key = null;
                value = null;
                return false;
            } else {
                return true;
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
            if (startOffset == end) {
                return 0.0f;
            } else {
                return Math.min(1.0f, (pos - startOffset) / (float) (end - startOffset));
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
