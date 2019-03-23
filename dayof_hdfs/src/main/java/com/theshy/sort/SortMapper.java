package com.theshy.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2121:30
 * com.theshy.sortbigdata
 */
public class SortMapper extends Mapper<LongWritable, Text,PairSort,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //通過context可以獲取到計數器
        Counter counter = context.getCounter("MAP_COUNTER","MAP_INPUT_RECORDS");
        //統計map階段輸入的數據
        counter.increment(1l);

        PairSort pairSort = new PairSort();
        String[] split = value.toString().split("\t");
        pairSort.setFirst(split[0]);
        pairSort.setSecond(Integer.parseInt(split[1]));
        context.write(pairSort, value);
    }
}
