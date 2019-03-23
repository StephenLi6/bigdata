package com.theshy.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2320:14
 * com.theshy.joinbigdata
 */
public class ReduceJoinMapper extends Mapper<LongWritable, Text,Text,Text> {
    Text text = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        String name = inputSplit.getPath().getName();

        String s = value.toString();
        if (s.startsWith("p")){
            String[] split = s.split(",");
            text.set(split[0]);
            context.write(text, value);
        }else{
            String[] split = s.split(",");
            text.set(split[2]);
            context.write(text, value);
        }
    }
}
