package com.carl.hadoop.config;

import com.carl.hadoop.mr.DefaultReducer;
import com.carl.hadoop.mr.WriteStrategy;
import com.carl.hadoop.run.RunnerParam;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import java.lang.reflect.Type;

/**
 * <p>Title: cn.carl.hadoop.config ReducerConfiuration</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 18:33
 * @Version 1.0
 */
public class ReducerConfiguration extends AbstractConfiguration {

    /**
     * 缓存当前的运行任务的上下文对象
     */
    private Reducer.Context REDUCERCONTEXT;

    /**
     * 保存当前输入数据的路径
     *
     * @param inputPath     输入数据路径
     * @param writeStrategy 当前写处数据的策略方式
     * @param runnerParam   运行参数对象
     * @param types         当前任务的四个参数类型
     */
    public ReducerConfiguration(String[] inputPath,
                                WriteStrategy writeStrategy,
                                RunnerParam runnerParam,
                                Type[] types) {

        //必要参数交给父类初始化
        super(inputPath, runnerParam, types);

        this.writeStrategy = writeStrategy;

        this.REDUCERCONTEXT = new DefaultReducer().getContext(writeStrategy);
    }

    /**
     * 使用默认策略的输出策略写出数据
     *
     * @param inputPath   输入数据路径
     * @param runnerParam 运行参数对象
     * @param types       当前任务的四个参数类型
     */
    public ReducerConfiguration(String[] inputPath,
                                RunnerParam runnerParam,
                                Type[] types) {
        this(inputPath, null, runnerParam, types);
    }

    @Override
    public TaskInputOutputContext getContext() {
        return REDUCERCONTEXT;
    }

    @Override
    public HadoopType getStatus() {
        return HadoopType.Reducer;
    }

    /**
     * 获取当前的参数对象
     *
     * @return 任务参数对象
     */
    @Override
    public RunnerParam getRunnerParam() {
        return this.runnerParam;
    }
}
