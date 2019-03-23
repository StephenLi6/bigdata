package com.theshy.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2020:14
 * com.theshy.wordcountbigdata
 */
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int a = 0;
        for (IntWritable value : values) {
            int i = value.get();
            a +=i;
        }
        context.write(key, new IntWritable(a));
    }
}
