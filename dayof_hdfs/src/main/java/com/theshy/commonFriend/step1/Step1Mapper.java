package com.theshy.commonFriend.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2420:11
 * com.theshy.commonFriend.step1bigdata
 *
 */
public class Step1Mapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        String[] split1 = split[1].split(",");
        for (String s : split1) {
            context.write(new Text(s), new Text(split[0]));
        }
    }
}
