package com.carl.hadoop.config;

import com.carl.hadoop.mr.DefaultMapper;
import com.carl.hadoop.mr.FileWrite;
import com.carl.hadoop.mr.WriteStrategy;
import com.carl.hadoop.run.RunnerParam;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.lang.reflect.Type;

/**
 * <p>Title: cn.carl.hadoop.config MapperConfiguration</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 18:33
 * @Version 1.0
 */
public class MapperConfiguration extends AbstractConfiguration {

    /**
     * 缓存当前的运行任务的上下文对象
     */
    private final Mapper.Context MAPPERCONTEXT;

    /**
     * 指定输入数据路径,获取当前Mapper任务运行的上下文对象
     *
     * @param inputPath 输入数据的路径
     * @return writeStrategy 当前任务运行的上下文对象
     */
    public MapperConfiguration(String[] inputPath, WriteStrategy writeStrategy,
                               RunnerParam runnerParam, Type[] types) {
        super(inputPath, runnerParam, types);

        if (null == writeStrategy) {
            writeStrategy = new FileWrite();
        }

        //保存当前写出策略方式对象
        this.writeStrategy = writeStrategy;

        //保存当前上下文对象
        this.MAPPERCONTEXT = new DefaultMapper().getContext(writeStrategy, this.fileSplitWrapper);
    }

    /**
     * 通过输入路径构造一个当前运行的上下文对象
     *
     * @param inputPath 数据输入路径
     */
    public MapperConfiguration(String[] inputPath, RunnerParam runnerParam, Type[] types) {
        this(inputPath, null, runnerParam, types);
    }

    /**
     * 获取当前的Mapper任务的上下文对象
     *
     * @return 获取当前任务的上下文对象
     */
    @Override
    public TaskInputOutputContext getContext() {
        return MAPPERCONTEXT;
    }

    /**
     * 获取当前上下文对象的类型
     *
     * @return 任务上下文类型
     */
    @Override
    public HadoopType getStatus() {
        return HadoopType.Mapper;
    }

    @Override
    public RunnerParam getRunnerParam() {
        return this.runnerParam;
    }
}
