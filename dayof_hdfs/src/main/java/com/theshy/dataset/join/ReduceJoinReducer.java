package com.theshy.dataset.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2320:27
 * com.theshy.joinbigdata
 */
public class ReduceJoinReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String firstPart ="";
        String secondPart = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")){
                firstPart = value.toString();
            }else{
                secondPart = value.toString();
            }

        }
            context.write(key, new Text(firstPart +"\t"+secondPart));
    }
}
