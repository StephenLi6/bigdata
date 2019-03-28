package com.theshy.dataset.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/1/2121:03
 * com.theshy.sortbigdata
 */
public class PairSort implements WritableComparable<PairSort> {
    private String first;
    private Integer second;

    @Override
    public String toString() {
        return "PairSort{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public int compareTo(PairSort o) {
        int i = this.first.compareTo(o.first);
        if (i != 0) {
            return i;
        }else{
            int i1 = this.second.compareTo(o.second);
            return i1;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(first);
        out.writeInt(second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }
}
