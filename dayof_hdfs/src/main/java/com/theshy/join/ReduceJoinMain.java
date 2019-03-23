package com.theshy.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2320:30
 * com.theshy.joinbigdata
 */
public class ReduceJoinMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(), "ReduceJoin");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///E:\\BoringThing\\hadoop加密视频1-4天\\4、第四天\\资料\\map端join\\input"));

        job.setMapperClass(ReduceJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(ReduceJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\GoGoing.000\\Desktop\\ccc"));
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ReduceJoinMain(), args);
        System.exit(run);
    }
}
