package com.theshy.dataset.commonFriend.step2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2420:41
 * com.theshy.commonFriend.step2bigdata
 */
public class Step2Mapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        String[] user = split[0].split("-");
        Arrays.sort(user);

        for (int i = 0; i < user.length-1; i++) {
            for (int j = i+1; j < user.length; j++) {
                context.write(new Text(user[i]+"-"+user[j]), new Text(split[1]));
            }
        }

    }
}
