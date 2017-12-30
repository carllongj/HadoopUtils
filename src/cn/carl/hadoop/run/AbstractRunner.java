package cn.carl.hadoop.run;

import cn.carl.hadoop.config.ContextConfig;
import cn.carl.hadoop.config.HadoopType;
import cn.carl.other.Assert;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;

/**
 * <p>Title: cn.carl.hadoop.run AbstractRunner</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 19:43
 * @Version 1.0
 */
public abstract class AbstractRunner implements HadoopRunner {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(AbstractRunner.class);

    /**
     * 保存当前任务运行的Class对象
     */
    protected final Class<?> mrClass;

    /**
     * 当前任务运行的参数对象,所有的数据都在此对象中
     */
    protected final ContextConfig contextConfig;

    /**
     * 获取读取下一个文件的数据的方法名称
     */
    private static final String getNextFile = "getNextFile";

    /**
     * 获取下一个文件数据的方法
     */
    private Method getNextFileMethod;

    /**
     * 当前MR任务类的包装实例
     */
    protected Object mrInstance;

    /**
     * 构造当前运行器对象必须的参数
     *
     * @param mrClass       运行任务的Class对象
     * @param contextConfig 当前运行任务的上下文对象
     */
    public AbstractRunner(Class<?> mrClass, ContextConfig contextConfig) {
        this.mrClass = mrClass;
        this.contextConfig = contextConfig;

        //初始化数据
        init();
    }

    /**
     * 获取当前任务运行的运行对象
     *
     * @param clazz         执行的Class对象
     * @param contextConfig 当前的配置对象
     * @return 当前的任务运行对象
     */
    public static HadoopRunner getHadoopRunner(Class<?> clazz, ContextConfig contextConfig) {

        //提供对外的方法,保证参数不为null
        Assert.assertNotNull(clazz);
        Assert.assertNotNull(contextConfig);

        //获取当前运行任务是Mapper类还是Reducer类
        if (contextConfig.getStatus() == HadoopType.Mapper) {

            //如果是Mapper类
            return new MapperRunner(clazz, contextConfig);

        } else if (contextConfig.getStatus() == HadoopType.Reducer) {

            //如果是Reducer类
            return new ReducerRunner(clazz, contextConfig);
        }

        LOGGER.error("current ContextConfig is not initialization");
        return null;
    }

    /**
     * 获取下一个文件的输入路径
     * 如果返回null.表示已经没有下一个文件
     *
     * @return 下一个文件的输入路径
     */
    @Override
    public String getNextFile() {

        //首先初始化一个空串,保证能获取到文件的时候不返回null
        String result = "";

        try {
            result = (String) this.getNextFileMethod.invoke(this.contextConfig.getFileSplitWrapper().
                    getWrapperInstance(), (Object[]) null);
        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not invoke the method named " + this.getNextFileMethod.getName());
        }

        return result;
    }

    /**
     * 初始化当前的一些类
     */
    private void init() {

        //获取当前获取文件名称的类
        final Class<?> instanceClass = this.contextConfig.getFileSplitWrapper().getWrapperClass();

        try {
            //获取当前获取下一个输入文件的方法对象
            this.getNextFileMethod = instanceClass.getMethod(getNextFile, (Class<?>[]) null);
        } catch (NoSuchMethodException e) {
            LOGGER.error("can not find a method named " + getNextFile);
            throw new RuntimeException(e.getMessage());
        }

        try {
            //获取当前运行任务的实例
            mrInstance = mrClass.newInstance();
        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not create instance of mrClass " + mrClass.getName());
            throw new RuntimeException(e.getMessage());
        }
    }
}
