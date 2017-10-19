package com.java.hadoop.mapreduce.fof;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by sgr on 2017/10/19/019.
 */
public class FOFPushSort extends WritableComparator {

    public FOFPushSort(){
        super(Friend.class,true);
    }

    /**
     * @param a
     * @param b
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Friend f1 = (Friend) a;
        Friend f2 = (Friend) b;

        int c = f1.getFriend1().compareTo(f2.getFriend1());
        if (c == 0){
            return -Integer.compare(f1.getHot(),f2.getHot());//亲密度降序排序
        }
        return c;
    }
}
