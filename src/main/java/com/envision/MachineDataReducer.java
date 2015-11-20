package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * Created by xujingnan on 15-11-13.
 */
public class MachineDataReducer extends Reducer<DoubleDescWritable, Text, NullWritable, Text> {
    private static final Log log = LogFactory.getLog(MachineDataReducer.class);
    private int[] reduceInputRecords;
    private OutputRecordIndex outputRecordIndex;
    private Integer nextIndex;
    private int point = -1;

    @Override
    protected void reduce(DoubleDescWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String tab = "\t";
        for (Text value : values) {
            point++;
            if (nextIndex != null) {
                if (point == nextIndex) {
                    Text tmp = new Text();
                    Long globalIndex = outputRecordIndex.getTotalOrderIndex(nextIndex);
                    tmp.append(globalIndex.toString().getBytes(), 0, globalIndex.toString().getBytes().length);
                    tmp.append(tab.toString().getBytes(), 0, tab.toString().getBytes().length);
                    tmp.append(key.toString().getBytes(), 0, key.toString().getBytes().length);
                    tmp.append(tab.getBytes(), 0, tab.getBytes().length);
                    tmp.append(value.getBytes(), 0, value.getBytes().length);
                    value.set(tmp);

                    context.write(NullWritable.get(), value);
                    nextIndex = outputRecordIndex.next();
                }
            } else {
                break;
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        int reduceNumber = context.getNumReduceTasks();
        reduceInputRecords = new int[reduceNumber];
        int reduceId = Integer.parseInt(Tools.getID(context.getTaskAttemptID().toString(), false));
        Configuration conf = context.getConfiguration();

        String line;
        try {
            Path parentPath = Tools.getReduceInputRecordsPath(context);
            FileSystem fs = parentPath.getFileSystem(conf);
            FileStatus[] fileStatuses = fs.listStatus(parentPath);
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

        int recordInterval = Tools.getInt(context, Tools.CONF_RECORD_INTERVAL, 10000000);
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
