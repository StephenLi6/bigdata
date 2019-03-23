package com.theshy.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2019:18
 * com.theshy.wordcountbigdata
 */
public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        //獲取一個job對象，用於我們任務的組織，通過job對象將我們八個步驟組織到一起，提交yarn集群運行
        Job job = Job.getInstance(super.getConf(), "xxx");

        //如果需要打包运行，一定得要加上这一句
        //job.setJarByClass(JobMain.class);

        job.setInputFormatClass(TextInputFormat.class);
        //集群模式運行，從hdfs上面讀取文件
        //TextInputFormat.addInputPath(job, new Path("hdfs://node1:8020/wordcount"));

        //使用本地模式来运行，从本地磁盘读取文件进行处理
        TextInputFormat.addInputPath(job,new Path("file:///C:\\Users\\GoGoing.000\\Desktop\\hadoop加密视频\\3、第三天\\资料\\wordcount\\input"));

        //第二步：自定義map邏輯，接收第一步的k1，v1。轉換成新的k2,v2。進行輸出
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //第七步：設置我們的reduce類，接收我們的key2，v2，輸出我們k3，v3
        job.setReducerClass(WordCountReducer.class);
        //設置key3輸出的類型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //第八步：設置我們的輸出類 outputformat
        job.setOutputFormatClass(TextOutputFormat.class);
        //TextOutputFormat.setOutputPath(job, new Path("hdfs://node1:8020/wordcountout"));

        //使用本地模式来运行
        TextOutputFormat.setOutputPath(job,new Path("file:///C:\\Users\\GoGoing.000\\Desktop\\hadoop加密视频\\3、第三天\\资料\\wordcount\\output3"));

        //提交任務
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name","local");
        configuration.set(" yarn.resourcemanager.hostname","local");

        int run = ToolRunner.run(configuration,new JobMain(), args);
        System.exit(run);
    }
}
