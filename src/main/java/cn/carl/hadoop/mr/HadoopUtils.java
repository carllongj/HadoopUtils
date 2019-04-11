package cn.carl.hadoop.mr;

import cn.carl.hadoop.config.HadoopParam;
import cn.carl.hadoop.config.MRConfig;
import cn.carl.hadoop.config.ContextConfig;
import cn.carl.hadoop.config.MapperConfiguration;
import cn.carl.hadoop.config.ReducerConfiguration;
import cn.carl.hadoop.run.AbstractRunner;
import cn.carl.hadoop.run.HadoopRunner;
import cn.carl.hadoop.run.ReducerParam;
import cn.carl.hadoop.run.RunnerParam;
import cn.carl.hadoop.utils.ReflectUtils;
import javassist.CannotCompileException;
import javassist.CtClass;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * 封装的一些Hadoop工具包,其中定义的初始化方法名称和回调方法名称需要是无参且为public
 * 此方法定义在MR程序中.
 * <p>Title: cn.carl.hadoop common</p>
 * <p>Description:
 * </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/1 22:15
 * @Version 1.0
 */
public class HadoopUtils {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(HadoopUtils.class);

    /**
     * 获取提供服务的类
     */
    private static final Provider PROVIDER = Provider.getInstance();

    /**
     * 写出当前给出类的Class文件
     *
     * @param clazz 写出当前mr的自定义的子类
     */
    public static void writeClass(Class<?> clazz, String path) {

        //要写的Class对象不能为null
        Objects.requireNonNull(clazz);

        //路径字符串不能为空
        if (!StringUtils.isBlank(path)) {
            return;
        }

        //创建对应的CtClass对象,默认根据当前项目路径下根据日期的毫秒数来建立一个文件
        CtClass ctClass =
                PROVIDER.createCtClass(clazz, new FileWrite(
                                System.getProperty("user.dir") +
                                        String.valueOf(System.currentTimeMillis())),
                        MRConfig.DEFAULT_PARTITION);

        try {
            //写出Class文件
            ctClass.writeFile(path);
        } catch (CannotCompileException e) {
            e.printStackTrace();
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * 调用含有定制化处理的MapReducer程序
     *
     * @param input         输入数据路径
     * @param output        输出数据路径
     * @param clazz         MR程序
     * @param writeStrategy 写的策略
     * @return 当前任务是否执行成功
     */
    public static boolean runMapReduce(String[] input, String output, Class<?> clazz, WriteStrategy writeStrategy, RunnerParam runnerParam) {
        return runHadoop(input, output, clazz, writeStrategy, runnerParam);
    }

    /**
     * 调用没有初始化和关闭方法的MR程序
     *
     * @param input         输入文件路径
     * @param output        输出数据路径
     * @param clazz         指定运行mr类
     * @param writeStrategy 写出数据方式
     */
    public static boolean runMapReduce(String[] input, String output, Class<?> clazz, WriteStrategy writeStrategy) {
        return runMapReduce(input, output, clazz, writeStrategy, null);
    }

    /**
     * 调用没有初始化和关闭方法的MR程序
     *
     * @param input  输入文件路径
     * @param output 输出数据路径
     * @param clazz  指定运行mr类
     */
    public static boolean runMapReduce(String[] input, String output, Class<?> clazz) {
        return runMapReduce(input, output, clazz, null);
    }

    /**
     * 用来本地运行mapreduce程序
     *
     * @param inputPaths     读入的输入文件目录
     * @param outPath        指定的输出文件目录
     * @param mapReduceClass 执行运行的类对象
     * @param writeStrategy  执行写出的策略方式
     * @param runnerParam    当前运行的一些参数
     * @return 任务是否执行成功
     */
    private static boolean runHadoop(String[] inputPaths, String outPath, Class<?> mapReduceClass,
                                     WriteStrategy writeStrategy, RunnerParam runnerParam) {

        //当前设置对象判空
        if (null == runnerParam) {
            runnerParam = new RunnerParam();
        }

        //声明当前的分区数量
        int partition = 0;

        //判断当前是否需要进行分区
        if (runnerParam instanceof ReducerParam) {
            ReducerParam param = (ReducerParam) runnerParam;
            partition = param.getPartition();
        }

        //此方法为最终执行方法不对参数进行校验,而由其他的方法来进行校验对应的参数
        Class<?> mrClass = PROVIDER.createClass(mapReduceClass, writeStrategy, partition);

        //没有写出策略
        if (null == writeStrategy) {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writeStrategy is null,create it depends on outputPtah");
            }
            //有输出路径
            if (StringUtils.isNotBlank(outPath)) {

                //写出默认策略并且将数据写出到指定的输出路径下
                writeStrategy = new FileWrite(outPath);
            } else {

                if (LOGGER.isDebugEnabled()) {
                    LOGGER.info("output path is empty,user default workspace to write file");
                }

                //没有输出路径,直接写出到工作空间下
                writeStrategy = new FileWrite();
            }
        }

        LOGGER.info("WriteStrategy initialize finish , starting build ContextConfig and Runner");

        //当前的运行环境对象
        ContextConfig contextConfig;

        Type[] types;

        //当前类是Mapper的子类
        if (ReflectUtils.isSubClass(mrClass, Mapper.class)) {

            LOGGER.info("current class is instance of " + HadoopParam.HADOOP_MAPPER_CLASS_NAME);

            //获取当前的运行参数类型
            types = ReflectUtils.getSuperGenericTypes(mrClass, HadoopParam.HADOOP_MAPPER_CLASS_NAME);

            LOGGER.info("build " + HadoopParam.HADOOP_MAPPER_CLASS_NAME + " ContextConfig");

            //当前写的策略为空,则默认使用文件写出
            contextConfig = new MapperConfiguration(inputPaths, writeStrategy, runnerParam, types);


        } else if (ReflectUtils.isSubClass(mrClass, Reducer.class)) {

            LOGGER.info("current class is instance of " + HadoopParam.HADOOP_REDUCER_CLASS_NAME);

            //获取当前运行参数类型
            types = ReflectUtils.getSuperGenericTypes(mrClass, HadoopParam.HADOOP_REDUCER_CLASS_NAME);

            //当前类是Reducer的子类

            //保存当前的运行时任务配置数据
            contextConfig = new ReducerConfiguration(inputPaths, writeStrategy, runnerParam, types);

        } else {
            LOGGER.error("不能执行的类,其不是Mapper或者Reducer的子类 : " + mrClass.getName());
            throw new RuntimeException("不能执行的类,其不是Mapper或者Reducer的子类 : " + mrClass.getName());
        }

        //获取当前运行的任务对象
        HadoopRunner runner = AbstractRunner.getHadoopRunner(mrClass, contextConfig);

        //当前对象判空
        if (null == runner) {
            LOGGER.error("obtain Hadoop Runner failed");
            return false;
        }

        //执行MR的逻辑
        return runner.runHadoop();
    }
}
