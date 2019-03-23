package com.theshy.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2121:41
 * com.theshy.sortbigdata
 */
public class SortMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(super.getConf(),"SortMap");
        job.setJarByClass(SortMain.class);
        //第一步：讀取文件，解析成key,value
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///C:\\Users\\GoGoing.000\\Desktop\\hadoop加密视频\\4、第四天\\资料\\排序\\input"));
        //第二步：設置mapper類
        job.setMapperClass(SortMapper.class);
        job.setMapOutputKeyClass(PairSort.class);
        job.setMapOutputValueClass(Text.class);
        //省略三到六步

        //設置第五步：規約：
        job.setCombinerClass(MyCombiner.class);

        //第七步：reduce階段
        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(PairSort.class);
        job.setOutputValueClass(NullWritable.class);
        //第八步：數據輸出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///C:\\Users\\GoGoing.000\\Desktop\\hadoop加密视频\\4、第四天\\资料\\排序\\outSort2"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SortMain(), args);
        System.exit(run);

    }
}
