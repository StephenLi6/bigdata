package com.theshy.dataset.mapJoin;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2321:01
 * com.theshy.mapJoinbigdata
 */
public class MapJoinMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    Map<String,String> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration configuration = context.getConfiguration();
        URI[] cacheFiles = DistributedCache.getCacheFiles(configuration);
        FileSystem fileSystem = FileSystem.get(cacheFiles[0],configuration );
        FSDataInputStream inputStream = fileSystem.open(new Path(cacheFiles[0]));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        String line = null;
        while ((line = bufferedReader.readLine()) != null){
            String[] split = line.split(",");
            map.put(split[0], line);
        }
        IOUtils.closeQuietly(inputStream);
        fileSystem.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        String product_line = map.get(split[2]);
        context.write(new Text(product_line+"\t"+value.toString()),NullWritable.get());
    }
}
