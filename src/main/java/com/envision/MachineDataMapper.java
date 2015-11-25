package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataMapper extends Mapper<DoubleDescWritable, Text, DoubleDescWritable, Text> {
    private final Log log = LogFactory.getLog(MachineDataMapper.class);
    public static ArrayList<Integer> reduceInputRecords;
    private int reduceNumber;
    private String mapIdString;
    private FSDataOutputStream out;
    private String inputSplitIdentifier;
    int recordIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        reduceNumber = context.getNumReduceTasks();
        reduceInputRecords = new ArrayList<Integer>(reduceNumber);
        for (int i = 0; i < reduceNumber; i++) {
            reduceInputRecords.add(0);
        }
        mapIdString = Tools.getID(context.getTaskAttemptID().toString(), true);
        inputSplitIdentifier = Tools.getInputSplitIdentifier((CombineFileSplit) context.getInputSplit());
        recordIndex = 0;
        log.info("task id:" + mapIdString);

        Configuration conf = context.getConfiguration();
        Path parent = Tools.getReduceInputRecordsPath(context);
        try {
            Path file = new Path(parent, mapIdString);
            log.info("map.out.file:" + file);
            FileSystem fs = file.getFileSystem(conf);
            out = fs.create(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(DoubleDescWritable key, Text value, Context context) throws IOException, InterruptedException {
        recordIndex++;
        super.map(key, new Text(inputSplitIdentifier + "." + Integer.toString(recordIndex)), context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        StringBuilder builder = new StringBuilder();
        builder.append(mapIdString);
        for (int i = 0; i < reduceNumber; i++) {
            builder.append(",");
            builder.append(reduceInputRecords.get(i));
        }
        builder.append("\n");
        log.info("records:" + builder.toString());
        InputStream in = new ByteArrayInputStream(builder.toString().getBytes());
        IOUtils.copyBytes(in, out, 4096, true);
        super.cleanup(context);
    }
}
