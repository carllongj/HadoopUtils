package com.carl.hadoop.mr;

import org.apache.hadoop.mapreduce.Counter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * <p>Title: cn.carl.hadoop.mr DefaultCounter</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 21:22
 * @Version 1.0
 */
public class DefaultCounterWrapper implements Counter {

    @Override
    public void setDisplayName(String s) {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getDisplayName() {
        return null;
    }

    @Override
    public long getValue() {
        return 0;
    }

    @Override
    public void setValue(long l) {

    }

    @Override
    public void increment(long l) {

    }

    @Override
    public Counter getUnderlyingCounter() {
        return null;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}
