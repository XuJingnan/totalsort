package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataReducer implements Reducer<DoubleDescWritable, Text, LongWritable, Text> {
    private static final Log log = LogFactory.getLog(MachineDataMapper.class);
    private int[] reduceInputRecords;
    private OutputRecordIndex outputRecordIndex;
    private Integer nextIndex;
    private int point = -1;

    @Override
    public void reduce(DoubleDescWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> collector, Reporter reporter) throws IOException {
        while (nextIndex != null && values.hasNext()) {
            Text value = values.next();
            point++;
            if (point == nextIndex) {
                //todo test value
                Text tmp = new Text();
                tmp.append(key.toString().getBytes(), 0, key.toString().getBytes().length);
                String tab = "\t";
                tmp.append(tab.getBytes(), 0, tab.getBytes().length);
                tmp.append(value.getBytes(), 0, value.getBytes().length);
                value.set(tmp);

                collector.collect(new LongWritable(outputRecordIndex.getTotalOrderIndex(nextIndex)), value);
                nextIndex = outputRecordIndex.next();
            }
        }
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf conf) {
        int reduceNumber = conf.getNumReduceTasks();
        reduceInputRecords = new int[reduceNumber];
        int reduceId = Integer.parseInt(MachineDataTool.getID(conf.get("mapred.task.id"), false));

        int mapNumber = conf.getNumMapTasks();
        String line;
        try {
            Path parentPath = MachineDataTool.getReduceInputRecordsPath(conf.get("mapred.output.dir"));
            FileSystem fs = parentPath.getFileSystem(conf);
            FileStatus[] fileStatuses = fs.listStatus(parentPath);
            if (fileStatuses.length != mapNumber) {
                log.error("map output reduceInputRecords error!!");
                return;
            }
            log.info("start to read all map out temp file");
            for (FileStatus file : fileStatuses) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
                line = reader.readLine().trim();
                reader.close();

                String[] tmp = line.split(",");
                for (int i = 1; i < tmp.length; i++) {
                    reduceInputRecords[i - 1] += Integer.parseInt(tmp[i]);
                }
            }
            log.info("end to read all map out temp file");
        } catch (IOException e) {
            e.printStackTrace();
        }

        int recordInterval = conf.getInt(MachineDataTool.RECORD_INTERVAL, 10000000);
        outputRecordIndex = new OutputRecordIndex(reduceId, recordInterval);
        nextIndex = outputRecordIndex.next();
    }

    class OutputRecordIndex {
        private int startPositionInTotalOrder = 1;
        private int endPositionInTotalOrder;
        ArrayList<Integer> index;
        int p = -1;

        public OutputRecordIndex(int reduceID, int recordInterval) {
            index = new ArrayList<Integer>();
            for (int i = 0; i < reduceID; i++) {
                startPositionInTotalOrder += reduceInputRecords[i];
            }
            endPositionInTotalOrder = startPositionInTotalOrder + reduceInputRecords[reduceID] - 1;
            int i = 1;
            log.info("startPos:" + startPositionInTotalOrder);
            log.info("endPos:" + endPositionInTotalOrder);
            while (i <= endPositionInTotalOrder) {
                if (i >= startPositionInTotalOrder) {
                    index.add(i - startPositionInTotalOrder);
                }
                i += recordInterval;
            }
            log.info("index to print:\t" + index);
        }

        public Integer next() {
            p++;
            if (index.size() == 0 || p >= index.size()) {
                return null;
            }
            return new Integer(index.get(p));
        }

        public Long getTotalOrderIndex(int index) {
            return Long.valueOf(index + startPositionInTotalOrder);
        }
    }
}
