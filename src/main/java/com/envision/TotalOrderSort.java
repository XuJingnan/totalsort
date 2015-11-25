package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/**
 * Created by xujingnan on 15-11-13
 */
public class TotalOrderSort extends Configured implements Tool {
    private static final Log log = LogFactory.getLog(TotalOrderSort.class);
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
     arg7:  seperatorFlag 0:"," 1:"\001" other:"\t"
     cmd for example:
     hadoop jar totalsort-1.3.jar com.envision.TotalOrderSort input/machinedata output/machinedata 1 2 100000 10 10 0
     */
    public int run(String[] args) throws Exception {
        log.info("starting");

        Configuration conf = getConf();
        Job sortJob = Job.getInstance(conf);
        conf = sortJob.getConfiguration();
        Path inputDir = new Path(args[0]);
        Path tmpDir = new Path(args[1] + ".tmp");
        Path outDir = new Path(args[1]);

        FileSystem fs = tmpDir.getFileSystem(conf);
        fs.delete(tmpDir, true);
        conf.setInt(Tools.CONF_START_POSITION, Integer.parseInt(args[2]));
        sortJob.setNumReduceTasks(Integer.parseInt(args[3]));
        sampleRecordNumbers = Integer.parseInt(args[4]);
        sampleSplitNumbers = Integer.parseInt(args[5]);
        conf.setInt(Tools.CONF_RECORD_INTERVAL, Integer.parseInt(args[6]));
        conf.setInt(Tools.CONF_SEPERATOR_FLAG, Integer.parseInt(args[7]));

        sortJob.setJobName("TotalOrderSort.sort");
        sortJob.setJarByClass(TotalOrderSort.class);
        sortJob.setMapperClass(MachineDataMapper.class);
        sortJob.setReducerClass(MachineDataReducer.class);

        sortJob.setInputFormatClass(MachineDataCombineFileInputFormat.class);
        sortJob.setMapOutputKeyClass(DoubleDescWritable.class);
        sortJob.setMapOutputValueClass(Text.class);
        sortJob.setOutputFormatClass(TextOutputFormat.class);
        sortJob.setOutputKeyClass(NullWritable.class);
        sortJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sortJob, inputDir);
        FileOutputFormat.setOutputPath(sortJob, tmpDir);
        inputDir = inputDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
        Path partitionFile = new Path(inputDir, Tools.PARTITION_FILENAME);
        sortJob.setPartitionerClass(TotalOrderPartitionWithCounter.class);
        TotalOrderPartitionWithCounter.setPartitionFile(conf, partitionFile);
        InputSampler.Sampler<DoubleDescWritable, Text> sampler = new InputSampler.SplitSampler<DoubleDescWritable, Text>(sampleRecordNumbers, sampleSplitNumbers);
        InputSampler.writePartitionFile(sortJob, sampler);
        Tools.printPartitionFile(conf);
        URI partitionUri = new URI(partitionFile.toString() + "#" + Tools.PARTITION_FILENAME);
        sortJob.addCacheFile(partitionUri);

        sortJob.waitForCompletion(true);

        Job outputJob = Job.getInstance(new Configuration());
        FileInputFormat.setInputPaths(outputJob, inputDir);
        Configuration outputJobConfig = outputJob.getConfiguration();
        outputJobConfig.set(Tools.TEMP_HDFS_PATH, tmpDir.toString());
        outputJobConfig.setInt(Tools.CONF_START_POSITION, Integer.parseInt(args[2]));
        outputJobConfig.setInt(Tools.CONF_SEPERATOR_FLAG, Integer.parseInt(args[7]));
        FileOutputFormat.setOutputPath(outputJob, outDir);
        fs = outDir.getFileSystem(outputJobConfig);
        fs.delete(outDir, true);
        outputJob.setJobName("TotalOrderSort.output");
        outputJob.setJarByClass(TotalOrderSort.class);
        outputJob.setMapperClass(MachineDataJoinMapper.class);
        outputJob.setNumReduceTasks(1);
        outputJob.setInputFormatClass(MachineDataCombineFileInputFormat.class);
        outputJob.setMapOutputKeyClass(LongWritable.class);
        outputJob.setMapOutputValueClass(Text.class);
        outputJob.setOutputFormatClass(TextOutputFormat.class);
        outputJob.setOutputKeyClass(LongWritable.class);
        outputJob.setOutputValueClass(Text.class);
        outputJob.waitForCompletion(true);

        log.info("done");
        return 0;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(null, new TotalOrderSort(), args);
        System.exit(res);
    }

}