package com.carl.hadoop.config;

/**
 * <p>Title: cn.carl.hadoop</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 20:56
 * @Version 1.0
 */
public interface HadoopParam {

    /**
     * 定义测试mr中key,value的输出的key,value的切分串
     */
    String KEY_VALUE_SPLIT_STANDARD = ",value ";

    /**
     * Hadoop中的Mapper的全限定名称
     */
    String HADOOP_MAPPER_CLASS_NAME = "org.apache.hadoop.mapreduce.Mapper";

    /**
     * Hadoop中的Mapper的上下文对象全限定名称
     */
    String HADOOP_MAPPER_CONTEXT_CLASS_NAME = "org.apache.hadoop.mapreduce.Mapper.Context";

    /**
     * Hadoop中的Reducer的全限定名称
     */
    String HADOOP_REDUCER_CLASS_NAME = "org.apache.hadoop.mapreduce.Reducer";

    /**
     * Hadoop中的Reducer的上下文对象的全限定名称
     */
    String HADOOP_REDUCER_CONTEXT_CLASS_NAME = "org.apache.hadoop.mapreduce.Reducer.Context";

    /**
     * LongWritable的全限定名称
     */
    String HADOOP_LONGWRITABLE_CLASS = "org.apache.hadoop.io.LongWritable";

    /**
     * 定义产生子类的全限定名称的前几个包
     */
    String SUB_CLASS_NAME = "com.carl.hadoop.provider.";

    /**
     * 定义产生子类的全限定名称的后缀
     */
    String SUB_CLASS_NAME_SUFFIX = "PROXYC";

    /**
     * 获取当前初始化数据的方法
     */
    String SETUP_METHOD = "setup";

    /**
     * 获取当前清空数据的方法
     */
    String CLEANUP_METHOD = "cleanup";

}
