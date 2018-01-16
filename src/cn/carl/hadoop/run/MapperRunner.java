package cn.carl.hadoop.run;

import cn.carl.hadoop.HadoopParam;
import cn.carl.hadoop.config.ContextConfig;
import cn.carl.io.CloseTools;
import cn.carl.io.IOTools;
import cn.carl.string.StringTools;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.lang.reflect.Method;

/**
 * <p>Title: cn.carl.hadoop.run MapperRunner</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 19:58
 * @Version 1.0
 */
public class MapperRunner extends AbstractRunner {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(MapperRunner.class);

    /**
     * 默认map的入口方法
     */
    private static final String MAP_INVOKE = "map";

    /**
     * 创建当前的Mapper运行的运行对象
     *
     * @param mrClass       执行Mapper的类
     * @param contextConfig 当前环境的配置
     */
    public MapperRunner(Class<?> mrClass, ContextConfig contextConfig) {
        super(mrClass, contextConfig);
    }

    @Override
    public boolean runHadoop() {

        LOGGER.info("prepare to execute Mapper");

        //当前任务执行的类
        final Object instance = this.mrInstance;

        String currentPath;

        //如果指定了初始化方法名,只执行初始化方法
        if (StringTools.hasText(this.contextConfig.getRunnerParam().getInitMethod())) {

            LOGGER.info("execute init method " + this.contextConfig.getRunnerParam().getInitMethod());

            invokeMethod(this.contextConfig.getRunnerParam().getInitMethod(), instance);
        }

        LOGGER.info("execute specific class of Mapper");

        while (null != (currentPath = this.getNextFile())) {

            LOGGER.info("current file path is " + currentPath);

            try {

                //如果需要执行初始化方法,执行初始化方法
                if (this.contextConfig.getRunnerParam().isExecuteSetup()) {
                    invokeContextMethod(HadoopParam.SETUP_METHOD, instance);
                }

                //获取该类实例中有没有对应的MultipleOutputs对象,作为多路径输出
                this.checkOutputs(mrClass, mrInstance);

                //执行核心的map方法
                invokeMap(instance, currentPath);

                //调用清空数据的方法
                if (this.contextConfig.getRunnerParam().isExecuteCleanup()) {
                    invokeContextMethod(HadoopParam.CLEANUP_METHOD, instance);
                }

            } catch (Exception e) {
                LOGGER.error("can not execute Class " + mrClass.getName());
                throw new RuntimeException(e.getMessage());
            }
        }

        //调用执行该类父类的回调方法
        if (StringTools.hasText(this.contextConfig.getRunnerParam().getCallback())) {
            LOGGER.info("execute the callback method of Mapper");
            invokeMethod(this.contextConfig.getRunnerParam().getCallback(), instance);
        }

        LOGGER.info("map execute successfully");
        return true;
    }

    /**
     * 调用当前处理任务的核心的map方法
     *
     * @param instance 当前执行任务的核心对象
     * @param path     输入文件数据的路径
     */
    private void invokeMap(Object instance, String path) {

        //设置当前的行数
        long lineNum = 0;

        LongWritable longWritable = new LongWritable(lineNum);

        //创建文本缓存对象
        Text text = new Text();

        BufferedReader reader = IOTools.createReader(path);

        if (null == reader) {
            throw new RuntimeException("can not read file " + path);
        }

        try {
            Method method = instance.getClass().getMethod(MAP_INVOKE, LongWritable.class,
                    Text.class, Mapper.Context.class);

            if (null == method) {
                throw new RuntimeException("can not find method named " + MAP_INVOKE);
            }
            String dataInput;

            while (null != (dataInput = reader.readLine())) {

                //获取当前的行数
                longWritable.set(++lineNum);

                text.set(dataInput);

                method.invoke(instance, longWritable, text, this.contextConfig.getContext());
            }
        } catch (Exception e) {
            LOGGER.error("can not execute method " + MAP_INVOKE + " of Class " + mrClass.getName());
            throw new RuntimeException("can not execute method " + MAP_INVOKE + " of Class " + mrClass.getName());
        } finally {
            CloseTools.closeStream(reader);
        }
    }

    /**
     * 获取当前执行指定名称的方法
     *
     * @param name     方法名
     * @param instance 当前的实例对象
     */
    private void invokeContextMethod(String name, Object instance) {
        try {
            Method method = instance.getClass().getDeclaredMethod(name, Mapper.Context.class);

            //执行当前的指定名称方法
            if (null != method) {
                method.invoke(instance, this.contextConfig.getContext());
            }

        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not invoke the method named " + name);
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * 调用类中的指定的方法名称
     * 该方法必须是public 且 无参数
     *
     * @param methodName 方法名
     * @param instance   当前对象的实例
     */
    private void invokeMethod(String methodName, Object instance) {
        try {
            instance.getClass().getMethod(methodName, (Class[]) null).invoke(instance, (Object[]) null);
        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not invoke method named " + methodName);
            LOGGER.error(e.getMessage());
        }
    }
}
