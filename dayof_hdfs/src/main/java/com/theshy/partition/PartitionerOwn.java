package com.theshy.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2120:08
 * com.theshy.partitionbigdata
 */
public class PartitionerOwn extends Partitioner<Text, NullWritable> {

    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] split = text.toString().split("\t");
        String gameResult = split[5];
        if (null != gameResult && "" != gameResult){
            if (Integer.parseInt(gameResult) > 15){
                return 0;
            }else{
                return 1;
            }
        }
        return 0;
    }
}
