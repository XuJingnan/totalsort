package com.envision;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by xujingnan on 15-11-13.
 */
public class DoubleDescWritable implements WritableComparable<DoubleDescWritable> {

    private double value = 0.0;

    public DoubleDescWritable() {
    }

    public DoubleDescWritable(double value) {
        set(value);
    }

    public void set(double value) {
        this.value = value;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        value = in.readDouble();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof DoubleDescWritable)) {
            return false;
        }
        DoubleDescWritable other = (DoubleDescWritable) o;
        return this.value == other.value;
    }

    @Override
    public int hashCode() {
        return (int) Double.doubleToLongBits(value);
    }

    @Override
    public int compareTo(DoubleDescWritable o) {
        return (value < o.value ? 1 : (value == o.value ? 0 : -1));
    }

    @Override
    public String toString() {
        return Double.toString(value);
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
