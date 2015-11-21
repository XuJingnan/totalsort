package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by xujingnan on 15-11-15.
 */
public class Tools {

    public static final String PATH_SUFFIX = ".reduce.input.records";
    public static final String CONF_RECORD_INTERVAL = "output.record.interval";
    // flag == 1 : seperate by "\001", otherwise by "\t"
    public static final String CONF_SEPERATOR_FLAG = "input.value.field.seperator.flag";
    public static final String CONF_START_POSITION = "start.position.to.sort";
    public static final String PARTITION_FILENAME = "_partition.lst";
    public static final int ADD_POSITION = 352;

    private static final Log log = LogFactory.getLog(Tools.class);

    public static Path getReduceInputRecordsPath(JobContext context) {
        String parent = FileOutputFormat.getOutputPath(context).toString();
        if (parent.endsWith("/")) {
            parent = parent.substring(0, parent.length() - 1);
        }
        parent += PATH_SUFFIX;
        return new Path(parent);
    }

    public static String getID(String mapredTaskID, boolean isMap) {
        if (isMap) {
            return mapredTaskID.split("m_")[1].split("_")[0];
        } else {
            return mapredTaskID.split("r_")[1].split("_")[0];
        }
    }

    public static int getInt(JobContext context, String name) {
        return getInt(context, name, 0);
    }

    public static int getInt(JobContext context, String name, int defaultValue) {
        return context.getConfiguration().getInt(name, defaultValue);
    }

    public static void setInt(JobContext context, String name, int value) {
        context.getConfiguration().setInt(name, value);
    }

    public static String getString(JobContext context, String name) {
        return context.getConfiguration().get(name, "");
    }

    public static void setString(JobContext context, String name, String value) {
        context.getConfiguration().set(name, value);
    }

    public static void increseRecord(int part) {
        int num = MachineDataMapper.reduceInputRecords.get(part);
        MachineDataMapper.reduceInputRecords.set(part, num + 1);
    }

    public static void printPartitionFile(Configuration conf) throws IOException {
        final Path partFile = new Path(TotalOrderPartitioner.getPartitionFile(conf));
        final FileSystem fs = ("_partition.lst".equals(partFile.toString()))
                ? FileSystem.getLocal(conf)     // assume in DistributedCache
                : partFile.getFileSystem(conf);
        Job job = Job.getInstance(conf);
        Class keyClass = job.getMapOutputKeyClass();
        ArrayList<DoubleDescWritable> splitPoints = readPartitions(fs, partFile, keyClass, conf);
        for (DoubleDescWritable point : splitPoints) {
            log.info("point:" + point);
        }
    }

    private static ArrayList<DoubleDescWritable> readPartitions(FileSystem fs, Path p, Class<DoubleDescWritable> keyClass,
                                                                Configuration conf) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, p, conf);
        ArrayList<DoubleDescWritable> parts = new ArrayList<DoubleDescWritable>();
        DoubleDescWritable key = ReflectionUtils.newInstance(keyClass, conf);
        NullWritable value = NullWritable.get();
        try {
            while (reader.next(key, value)) {
                parts.add(key);
                key = ReflectionUtils.newInstance(keyClass, conf);
            }
        } finally {
            reader.close();
        }
        return parts;
    }
}
