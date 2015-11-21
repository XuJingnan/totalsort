package com.envision;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
     hadoop jar totalsort-1.1.jar com.envision.TotalOrderSort input/machinedata output/machinedata 1 10 100000 10 10 0
     */
    public int run(String[] args) throws Exception {
        log.info("starting");

        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        conf = job.getConfiguration();

        Path inputDir = new Path(args[0]);
        Path outDir = new Path(args[1]);
        FileSystem fs = outDir.getFileSystem(conf);
        fs.delete(outDir, true);
        conf.setInt(Tools.CONF_START_POSITION, Integer.parseInt(args[2]));
        job.setNumReduceTasks(Integer.parseInt(args[3]));
        sampleRecordNumbers = Integer.parseInt(args[4]);
        sampleSplitNumbers = Integer.parseInt(args[5]);
        conf.setInt(Tools.CONF_RECORD_INTERVAL, Integer.parseInt(args[6]));
        conf.setInt(Tools.CONF_SEPERATOR_FLAG, Integer.parseInt(args[7]));

        job.setJobName("TotalOrderSort");
        job.setJarByClass(TotalOrderSort.class);
        job.setMapperClass(MachineDataMapper.class);
        job.setReducerClass(MachineDataReducer.class);

        job.setInputFormatClass(MachineDataCombineFileInputFormat.class);
        job.setMapOutputKeyClass(DoubleDescWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outDir);
        inputDir = inputDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
        Path partitionFile = new Path(inputDir, Tools.PARTITION_FILENAME);
        job.setPartitionerClass(TotalOrderPartitionWithCounter.class);
        TotalOrderPartitionWithCounter.setPartitionFile(conf, partitionFile);
        InputSampler.Sampler<DoubleDescWritable, Text> sampler = new InputSampler.SplitSampler<DoubleDescWritable, Text>(sampleRecordNumbers, sampleSplitNumbers);
        InputSampler.writePartitionFile(job, sampler);
        Tools.printPartitionFile(conf);
        URI partitionUri = new URI(partitionFile.toString() + "#" + Tools.PARTITION_FILENAME);
        job.addCacheFile(partitionUri);

        job.waitForCompletion(true);
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