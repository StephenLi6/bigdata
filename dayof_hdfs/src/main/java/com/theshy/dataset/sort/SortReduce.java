package com.theshy.dataset.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2121:38
 * com.theshy.sortbigdata
 */
public class SortReduce extends Reducer<PairSort, Text,PairSort, NullWritable> {

    public enum Counter{
        REDUCE_INPUT_KEY_TOAL,
        REDUCE_INPUT_VALUE_TOAL
    }

    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.REDUCE_INPUT_KEY_TOAL).increment(1l);
        for (Text value : values) {
            context.getCounter(Counter.REDUCE_INPUT_VALUE_TOAL).increment(1l);
            context.write(key, NullWritable.get());
        }
    }
}
