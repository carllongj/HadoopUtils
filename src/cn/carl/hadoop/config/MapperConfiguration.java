package cn.carl.hadoop.config;

import cn.carl.hadoop.mr.DefaultMapper;
import cn.carl.hadoop.mr.FileWrite;
import cn.carl.hadoop.mr.WriteStrategy;
import cn.carl.hadoop.run.RunnerParam;
import cn.carl.hadoop.wrapper.ConfigurationWrapper;
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
    private final Mapper.Context mapperContext;

    /**
     * 指定输入数据路径,获取当前Mapper任务运行的上下文对象
     *
     * @param inputPath     输入数据的路径
     * @param outputPath    输出数据的路径
     * @param writeStrategy 写出数据的方式
     * @param runnerParam   任务运行的参数
     * @param types         泛型参数的集合
     */
    public MapperConfiguration(String[] inputPath, String outputPath,
                               WriteStrategy writeStrategy,
                               RunnerParam runnerParam, Type[] types) {
        super(inputPath, outputPath, runnerParam, types);

        if (null == writeStrategy) {
            writeStrategy = new FileWrite();
        }

        //保存当前写出策略方式对象
        this.writeStrategy = writeStrategy;

        //保存当前上下文对象
        this.mapperContext = new DefaultMapper().getContext(this);

        //添加配置文件路径
        this.configPath = runnerParam.getConfigPaths();

        this.configuration = new ConfigurationWrapper(this.configPath);
    }

    /**
     * 通过输入路径构造一个当前运行的上下文对象
     *
     * @param inputPath   数据输入路径
     * @param outputPath  数据输出数据路径
     * @param runnerParam 运行的参数集合
     * @param types       当前的泛型参数集合
     */
    public MapperConfiguration(String[] inputPath, String outputPath,
                               RunnerParam runnerParam, Type[] types) {
        this(inputPath, outputPath, null, runnerParam, types);
    }

    /**
     * 获取当前的Mapper任务的上下文对象
     *
     * @return 获取当前任务的上下文对象
     */
    @Override
    public TaskInputOutputContext getContext() {
        return mapperContext;
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
