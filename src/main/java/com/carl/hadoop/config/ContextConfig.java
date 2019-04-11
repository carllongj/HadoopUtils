package com.carl.hadoop.config;

import com.carl.hadoop.mr.FileSplitWrapper;
import com.carl.hadoop.mr.WriteStrategy;
import com.carl.hadoop.run.RunnerParam;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.lang.reflect.Type;

/**
 * 用来获取当前任务运行的上下文对象
 * <p>Title: cn.carl.hadoop.config ContextHolder</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 18:34
 * @Version 1.0
 */
public interface ContextConfig {

    /**
     * 获取当前上下文对象
     *
     * @return
     */
    TaskInputOutputContext getContext();

    /**
     * 获取当前上下文的类型
     *
     * @return 返回当前的任务类型
     */
    HadoopType getStatus();

    /**
     * 获取当前的写出策略数据
     *
     * @return
     */
    WriteStrategy getWriteStrategy();

    /**
     * 获取当前数据切分信息,通过此对象来获取当前数据的文件名
     *
     * @return FileSplitWrapper类对象
     */
    FileSplitWrapper getFileSplitWrapper();

    /**
     * 返回当前的运行的一些参数
     *
     * @return 当前的运行参数对象
     */
    RunnerParam getRunnerParam();

    /**
     * 获取当前任务运行的泛型类型参数
     *
     * @return 泛型参数类型数组
     */
    Type[] getGenericParameters();
}
