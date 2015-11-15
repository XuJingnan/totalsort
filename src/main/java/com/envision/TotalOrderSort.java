package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by xujingnan on 15-11-13
 */
public class TotalOrderSort extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(TotalOrderSort.class);
    public static final int ADD_POSITION = 352;
    public static final String CONF_START_POSITION = "start.position";
    public static final String PARTITION_FILENAME = "_partition.lst";
    public static int sampleRecordNumbers;
    public static int sampleSplitNumbers;

    /*
     arg0:  inputPath
     arg1:  outputPath
     arg2:  startPosition(>=1)
     arg3:  reduceNumber
     arg4:  sampleRecordNumbers
     arg5:  sampleSplitNumbers
     arg6:  recordInterval
     cmd for example:
     hadoop jar totalsort-1.0-SNAPSHOT-jar-with-dependencies.jar com.envision.TotalOrderSort
     input/machinedata output/machinedata 1 10 100000 10 10
     */
    public int run(String[] args) throws Exception {
        LOG.info("starting");

        JobConf conf = (JobConf) getConf();

        Path inputDir = new Path(args[0]);
        Path outDir = new Path(args[1]);
        outDir.getFileSystem(conf).delete(outDir, true);
        conf.setInt(CONF_START_POSITION, Integer.parseInt(args[2]));
        conf.setNumReduceTasks(Integer.parseInt(args[3]));
        if (args.length >= 7) {
            sampleRecordNumbers = Integer.parseInt(args[4]);
            sampleSplitNumbers = Integer.parseInt(args[5]);
            conf.setInt(MachineDataTool.RECORD_INTERVAL, Integer.parseInt(args[6]));
        }

        conf.setJobName("TotalOrderSort");
        conf.setJarByClass(TotalOrderSort.class);
        conf.setMapperClass(MachineDataMapper.class);
        conf.setReducerClass(MachineDataReducer.class);
        conf.setInt("dfs.replication", 1);
        conf.setJobPriority(JobPriority.VERY_HIGH);

        conf.setInputFormat(MachineDataInputFormat.class);
        conf.setMapOutputKeyClass(DoubleDescWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputFormat(MachineDataOutputFormat.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(conf, inputDir);
        FileOutputFormat.setOutputPath(conf, outDir);
        inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
        Path partitionFile = new Path(inputDir, PARTITION_FILENAME);
        conf.setPartitionerClass(TotalOrderPartitionWithCounter.class);
        TotalOrderPartitionWithCounter.setPartitionFile(conf, partitionFile);
        InputSampler.Sampler<DoubleDescWritable, Text> sampler = new InputSampler.SplitSampler<DoubleDescWritable, Text>(sampleRecordNumbers, sampleSplitNumbers);
        InputSampler.writePartitionFile(conf, sampler);
        URI partitionUri = new URI(partitionFile.toString() + "#" + PARTITION_FILENAME);
        DistributedCache.addCacheFile(partitionUri, conf);
        DistributedCache.createSymlink(conf);

        MachineDataOutputFormat.setFinalSync(conf, true);
        JobClient.runJob(conf);
        LOG.info("done");
        return 0;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new JobConf(), new TotalOrderSort(), args);
        System.exit(res);
    }

}