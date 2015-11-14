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

import java.io.BufferedInputStream;
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
    private String mapTaskId;
    private FSDataOutputStream out;

    @Override
    public void configure(JobConf conf) {
        super.configure(conf);

        final String PATH_SUFFIX = ".reduce.input.records";
        final String FILE_NAME = "records";

        reduceNumber = conf.getNumReduceTasks();
        reduceInputRecords = new ArrayList<Integer>(reduceNumber);
        for (int i = 0; i < reduceNumber; i++) {
            reduceInputRecords.add(0);
        }
        mapTaskId = conf.get("mapred.task.id");
        log.info("task id:" + mapTaskId);

        String reduceInputRecordsDir = conf.get("mapred.output.dir");
        if (reduceInputRecordsDir.endsWith("/")) {
            reduceInputRecordsDir = reduceInputRecordsDir.substring(0, reduceInputRecordsDir.length() - 1);
        }
        reduceInputRecordsDir = reduceInputRecordsDir.concat(PATH_SUFFIX);
        log.info("dir:" + reduceInputRecordsDir);
        try {
            Path dir = new Path(reduceInputRecordsDir);
            FileSystem fs = dir.getFileSystem(conf);
            if (!fs.exists(dir)) {
                log.info("create dir");
                fs.mkdirs(dir);
            }
            Path file = new Path(dir, FILE_NAME);
//            conf.setBoolean("dfs.support.append", true);
//            out = fs.append(file);
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
        builder.append(mapTaskId);
        for (int i = 0; i < reduceNumber; i++) {
            builder.append(",");
            builder.append(reduceInputRecords.get(i));
        }
        //write ${mapred.output.dir}.reduce.input.records/records
        log.info("records:" + builder.toString());
//        InputStream in = new ByteArrayInputStream(builder.toString().getBytes());
//        IOUtils.copyBytes(in, out, 4096, true);
        //todo incr counter

    }
}
