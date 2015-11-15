package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityMapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataMapper extends IdentityMapper<DoubleDescWritable, Text> {
    private final Log log = LogFactory.getLog(MachineDataMapper.class);
    public static ArrayList<Integer> reduceInputRecords;
    private int reduceNumber;
    private String mapIdString;
    private FSDataOutputStream out;

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);

        reduceNumber = conf.getNumReduceTasks();
        reduceInputRecords = new ArrayList<Integer>(reduceNumber);
        for (int i = 0; i < reduceNumber; i++) {
            reduceInputRecords.add(0);
        }
        mapIdString = MachineDataTool.getID(conf.get("mapred.task.id"), true);
        log.info("task id:" + mapIdString);

        Path parent = MachineDataTool.getReduceInputRecordsPath(conf.get("mapred.output.dir"));
        try {
            Path file = new Path(parent, mapIdString);
            log.info("map.out.file:" + file.toUri().toString());
            FileSystem fs = file.getFileSystem(conf);
            out = fs.create(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void map(DoubleDescWritable key, Text val, OutputCollector<DoubleDescWritable, Text> output, Reporter reporter) throws IOException {
        super.map(key, val, output, reporter);
    }

    @Override
    public void close() throws IOException {
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
    }
}
