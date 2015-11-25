package com.envision;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

/**
 * Created by xujingnan on 11/25/15.
 */
public class MachineDataJoinMapper extends Mapper<DoubleDescWritable, Text, LongWritable, Text> {

    private HashMap<String, String> records = new HashMap<String, String>();
    private String inputSplitIdentifier;
    int recordIndex;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        inputSplitIdentifier = Tools.getInputSplitIdentifier((CombineFileSplit) context.getInputSplit());
        recordIndex = 0;
        Path tmpPath = new Path(context.getConfiguration().get(Tools.TEMP_HDFS_PATH));
        FileSystem fs = tmpPath.getFileSystem(context.getConfiguration());
        String line;
        for (FileStatus file : fs.listStatus(tmpPath)) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
            while ((line = reader.readLine()) != null) {
                String[] tmp = line.split(Tools.TAB);
                records.put(tmp[2], tmp[0] + Tools.TAB + tmp[1]);
            }
            reader.close();
        }
    }

    @Override
    protected void map(DoubleDescWritable key, Text value, Context context) throws IOException, InterruptedException {
        recordIndex++;
        String id = inputSplitIdentifier + "." + Integer.toString(recordIndex);
        if (records.containsKey(id)) {
            String[] tmp = records.get(id).split(Tools.TAB);
            //tmp[0]: global order index, tmp[1]: tagx value
            context.write(new LongWritable(Long.parseLong(tmp[0])), new Text(tmp[1] + Tools.TAB + value.toString()));
        }
    }
}
