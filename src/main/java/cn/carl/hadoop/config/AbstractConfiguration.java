package cn.carl.hadoop.config;

import cn.carl.hadoop.mr.FileSplitWrapper;
import cn.carl.hadoop.mr.WriteStrategy;
import cn.carl.hadoop.run.RunnerParam;

import java.lang.reflect.Type;

/**
 * 抽象配置类,方便子类对Mapper和Reducer做特殊处理
 * <p>Title: cn.carl.hadoop.config MRConfiguration</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 18:03
 * @Version 1.0
 */
public abstract class AbstractConfiguration implements ContextConfig {

    /**
     * 当前的输入数据路径集合
     */
    protected final String[] inputPath;

    /**
     * 获取当前文件的写出策略方式
     */
    protected WriteStrategy writeStrategy;

    /**
     * 获取当前文件的切分方式,通过此获取数据文件名称
     */
    protected final FileSplitWrapper fileSplitWrapper;

    /**
     * 获取当前任务的运行参数
     */
    protected final RunnerParam runnerParam;

    /**
     * 当前mapreduce的四个键类型
     */
    protected final Type[] types;

    /**
     * 保存当前输入数据的路径
     *
     * @param inputPath
     */
    protected AbstractConfiguration(String[] inputPath, RunnerParam runnerParam, Type[] types) {
        if (null == runnerParam) {
            runnerParam = new RunnerParam();
        }

        this.inputPath = inputPath;
        this.runnerParam = runnerParam;

        //初始化的当前的数据,定义当前的FileSplit对象
        this.fileSplitWrapper = new FileSplitWrapper(this.inputPath);

        //保存当前的数据类型
        this.types = types;
    }

    /**
     * 直接返回当前的写出数据的策略
     *
     * @return 写出策略的方式
     */
    @Override
    public final WriteStrategy getWriteStrategy() {
        return this.writeStrategy;
    }

    /**
     * 获取当前的文件切分策略,通过此对象来获取输入数据文件名
     *
     * @return 包装FileSplit类
     */
    @Override
    public final FileSplitWrapper getFileSplitWrapper() {
        return this.fileSplitWrapper;
    }

    /**
     * 获取当前任务运行时的参数类型数组
     *
     * @return 类型数组
     */
    @Override
    public Type[] getGenericParameters() {
        return this.types;
    }
}
