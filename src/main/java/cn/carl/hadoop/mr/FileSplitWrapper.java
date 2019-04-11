package cn.carl.hadoop.mr;

import cn.carl.hadoop.config.AbstractClassMaker;
import cn.carl.hadoop.utils.AssistUtils;
import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtField;
import javassist.CtMethod;
import org.apache.log4j.Logger;

import java.lang.reflect.Constructor;

/**
 * 对当前数据提供获取文件名的操作
 * <p>Title: cn.carl.hadoop.mr FileSplitWrapper</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/26 21:21
 * @Version 1.0
 */
public class FileSplitWrapper extends AbstractClassMaker {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(FileSplitWrapper.class);

    /**
     * 当前指定的输入数据路径数组
     */
    private final String[] inputPath;

    /**
     * Hadoop中FileSplit的全限定类名称
     */
    private static final String HADOOP_FILE_SPLIT_CLASS =
            "org.apache.hadoop.mapreduce.lib.input.FileSplit";

    /**
     * 缓存当前已经加载过的CtClass对象
     */
    private static final CtClass CACHE;

    /**
     * 保存当前已经加载过的Class对象,如果再次调用CACHE.toClass会出错
     */
    private static final Class<?> CLAZZ;

    /**
     * 当前初始化后的实例对象
     */
    private Object splitInstance;

    /**
     * 只有当前包下的类才能使用当前类
     *
     * @param inputPath 指定的所有输入数据路径
     */
    public FileSplitWrapper(String[] inputPath) {
        this.inputPath = inputPath;

        //判空数据
        if (null == inputPath) {
            throw new NullPointerException("input path can not be null");
        }

        LOGGER.info("current path size " + inputPath.length);
        //给当前对象赋予一个的指定类的实例
        init();
    }

    static {
        //获取当前要改造的类的CtClass对象
        CtClass ctClass = AssistUtils.getCtClassByName(HADOOP_FILE_SPLIT_CLASS);

        //添加字段
        addFields(ctClass);

        //添加一个构造器
        addConstructor(ctClass);

        //添加所有的方法实现
        addMethods(ctClass);

        //当前的CtClass对象
        CACHE = ctClass;

        try {

            //让虚拟机加载此类
            CLAZZ = CACHE.toClass();

        } catch (CannotCompileException e) {
            LOGGER.error("can not compile class " + CACHE.getName());
            throw new RuntimeException("can not compile class " + CACHE.getName());
        }

    }

    /**
     * 给Hadoop的FileSplit类添加两个字段
     *
     * @param ctClass 当前的FileSplit的CtClass类
     */
    private static void addFields(CtClass ctClass) {

        //定义两个属性的字面量
        String positionField = "private int position;";
        String inputPathField = "private final String[] inputPath;";

        try {

            //给对应的类增加两个属性
            CtField position = CtField.make(positionField, ctClass);
            CtField inputPath = CtField.make(inputPathField, ctClass);

            //将两个字段添加到类中
            ctClass.addField(position);
            ctClass.addField(inputPath);

        } catch (CannotCompileException e) {
            LOGGER.error("can not add filed on " + ctClass.getName());
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 给指定的Hadoopde CtClass类对象添加一个构造器
     *
     * @param ctClass 指定的FileSplit的CtClass类对象
     */
    private static void addConstructor(CtClass ctClass) {
        //构造器体必须用 {} 括起来,并且如果有参数,在方法体中使用$123..来获取
        String body = "{this.inputPath = $1;this.position = 0;}";

        //获取一个字符串数组的CtClass对象
        CtClass[] param = new CtClass[]{
                AssistUtils.getCtClassByName("[Ljava.lang.String;")};

        //添加构造器
        AssistUtils.addConstructor(body, param, ctClass);
    }

    /**
     * 添加对应的方法和修改调用的方法
     *
     * @param ctClass 当前的Hadoop的FileSplit的CtClass对象
     */
    private static void addMethods(CtClass ctClass) {
        String reset = "public void reset(){this.position = 0;}";

        String getNextFile = "public String getNextFile(){if(this.position < this.inputPath.length)" +
                "{return this.inputPath[position++];} return null;}";

        String getPosition = "public int getPosition(){return this.position;}";

        String getPath = "public org.apache.hadoop.fs.Path getPath(){if(this.position < " +
                "this.inputPath.length){return new org.apache.hadoop.fs.Path(this.inputPath[position]);}return null;}";

        String methodName = "getPath";

        try {
            CtMethod resetMethod = CtMethod.make(reset, ctClass);
            CtMethod getNextFileMethod = CtMethod.make(getNextFile, ctClass);
            CtMethod getPositionMethod = CtMethod.make(getPosition, ctClass);
            CtMethod ctMethod = CtMethod.make(getPath, ctClass);

            //获取调用的方法
            CtMethod getPathMethod = AssistUtils.getMethod(ctClass, methodName, null);

            //重置方法体内容
            getPathMethod.setBody(ctMethod, null);

            //添加对应的方法
            ctClass.addMethod(resetMethod);
            ctClass.addMethod(getNextFileMethod);
            ctClass.addMethod(getPositionMethod);
        } catch (CannotCompileException e) {
            LOGGER.error("can not compile class " + HADOOP_FILE_SPLIT_CLASS);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public CtClass getCtClass() {
        return CACHE;
    }

    @Override
    public Class<?> getWrapperClass() {
        return CLAZZ;
    }

    @Override
    public Object getWrapperInstance() {
        return this.splitInstance;
    }

    /**
     * 初始化当前类的实例
     */
    private void init() {
        try {
            if (null != CLAZZ) {
                Constructor<?> constructor = CLAZZ.getConstructor(String[].class);
                Object instance = constructor.newInstance((Object) this.inputPath);
                this.splitInstance = instance;
            }
        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not create the instance " + CLAZZ.getName());
            throw new RuntimeException("can not create the instance " + CLAZZ.getName());
        }
    }
}
