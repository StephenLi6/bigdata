package com.theshy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2019:48
 * com.theshy.wordcountbigdata
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] split = line.split(",");
        for (String word : split) {
            text.set(word);
            intWritable.set(1);
            context.write(text, intWritable);
        }
    }
}
