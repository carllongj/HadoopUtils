package com.carl.hadoop.mr;

import java.io.Closeable;

/**
 * <p>Title: cn.carl.hadoop.mr</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 21:00
 * @Version 1.0
 */
public interface WriteStrategy extends Closeable {

    /**
     * 对write方法的一种策略模式抽象
     *
     * @param o1 写出的键数据
     * @param o2 写出的值的数据
     */
    void write(Object o1, Object o2);

    /**
     * 获取切分输出数据的关键切分数据
     *
     * @return 切分数据分隔符
     */
    String getSplitValue();
}
