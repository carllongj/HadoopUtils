package com.carl.hadoop.mr;

import com.carl.hadoop.config.MRConfig;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 默认的分区处理
 * <p>Title: cn.carl.hadoop.mr DefaultPartitioner</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/29 20:50
 * @Version 1.0
 */
public class DefaultPartitioner extends Partitioner {

    @Override
    public int getPartition(Object o, Object o2, int i) {
        return MRConfig.DEFAULT_PARTITION;
    }
}
