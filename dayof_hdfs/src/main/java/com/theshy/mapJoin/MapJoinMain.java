package com.theshy.mapJoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2321:33
 * com.theshy.mapJoinbigdata
 */
public class MapJoinMain extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = super.getConf();
        DistributedCache.addCacheFile(new URI("hdfs://node1:8020/product_cache/pdts.txt"), conf);
        Job job = Job.getInstance(conf, "mapJoin");
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("file:///E:\\BoringThing\\hadoop加密视频1-4天\\4、第四天\\资料\\map端join\\map_join_iput"));

        job.setMapperClass(MapJoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);


        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path("file:///E:\\BoringThing\\hadoop加密视频1-4天\\4、第四天\\资料\\map端join\\map_join_iput2222222222"));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new MapJoinMain(), args);
        System.exit(run);
    }
}
