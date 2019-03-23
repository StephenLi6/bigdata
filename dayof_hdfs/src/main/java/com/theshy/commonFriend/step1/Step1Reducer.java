package com.theshy.commonFriend.step1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2420:15
 * com.theshy.commonFriend.step1bigdata
 */
public class Step1Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
            buffer.append(value.toString()).append("-");
        }
        context.write(new Text(buffer.toString()), key);
    }
}
