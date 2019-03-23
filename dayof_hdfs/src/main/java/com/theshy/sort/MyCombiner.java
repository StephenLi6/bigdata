package com.theshy.sort;

import javafx.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2214:30
 * com.theshy.sortbigdata
 */
public class MyCombiner extends Reducer<PairSort, Text,PairSort,Text> {
    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
