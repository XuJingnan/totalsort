package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Progressable;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataOutputFormat extends TextOutputFormat<LongWritable, Text> {
    private static final Log log = LogFactory.getLog(MachineDataOutputFormat.class);
    static final String CONF_FINAL_SYNC = "final.sync";

    /**
     * Set the requirement for a final sync before the stream is closed.
     */
    public static void setFinalSync(JobConf conf, boolean newValue) {
        conf.setBoolean(CONF_FINAL_SYNC, newValue);
    }

    /**
     * Does the user want a final sync at close?
     */
    public static boolean getFinalSync(JobConf conf) {
        return conf.getBoolean(CONF_FINAL_SYNC, false);
    }

    class MachineDataRecordWriter extends LineRecordWriter<LongWritable, Text> {
        private final byte[] tab = "\t".getBytes();
        private final byte[] newLine = "\r\n".getBytes();
        private boolean finalSync = false;

        public MachineDataRecordWriter(DataOutputStream out,
                                       JobConf conf) {
            super(out);
            finalSync = getFinalSync(conf);
        }

        public synchronized void write(LongWritable key,
                                       Text value) throws IOException {
            byte[] keyBytes = key.toString().getBytes();
            out.write(keyBytes, 0, keyBytes.length);
            out.write(tab, 0, tab.length);
            out.write(value.getBytes(), 0, value.getLength());
            out.write(newLine, 0, newLine.length);
        }

        public synchronized void close(Reporter reporter) throws IOException {
            if (finalSync) {
                ((FSDataOutputStream) out).sync();
            }
            super.close(reporter);
        }
    }

    public RecordWriter<LongWritable, Text> getRecordWriter(FileSystem ignored, JobConf job, String name, Progressable progress) throws IOException {
        Path dir = getWorkOutputPath(job);
        FileSystem fs = dir.getFileSystem(job);
        FSDataOutputStream fileOut = fs.create(new Path(dir, name), progress);
        return new MachineDataRecordWriter(fileOut, job);
    }
}
