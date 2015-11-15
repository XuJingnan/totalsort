package com.envision;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by xujingnan on 15-11-13.
 */
public class DoubleDescWritable extends DoubleWritable {

    @Override
    public int compareTo(Object o) {
        return -super.compareTo(o);
    }

    public static class Comparator extends WritableComparator {

        public Comparator() {
            super(DoubleDescWritable.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            double thisValue = readDouble(b1, s1);
            double thatValue = readDouble(b2, s2);
            return (thisValue < thatValue ? 1 : (thisValue == thatValue ? 0 : -1));
        }
    }

    static {
        WritableComparator.define(DoubleDescWritable.class, new Comparator());
    }
}
