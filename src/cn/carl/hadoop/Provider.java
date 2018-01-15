package cn.carl.hadoop;

import cn.carl.aop.javassist.AssistTools;
import cn.carl.hadoop.mr.WriteStrategy;
import cn.carl.reflect.ReflectUtils;
import javassist.*;
import org.apache.log4j.Logger;

import java.lang.reflect.Type;

/**
 * 提供处理mr测试流程的工具核心类
 * <p>Title: cn.carl.hadoop.mr Provider</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 21:26
 * @Version 1.0
 */
class Provider {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(Provider.class);

    /**
     * Hadoop中处理Map的Class对象
     */
    private final static Class<?> HADOOP_MAPPER_CLASS;

    /**
     * Hadoop中处理Reduce的Class对象
     */
    private final static Class<?> HADOOP_REDUCER_CLASS;

    /**
     * 指定当前的方法体的字符串数据
     * 其中索引位置分别为(从0开始):
     * 1    -> 方法名称
     * 3    -> 上下文的具体类型
     * 5    -> 方法名称
     */
    private static final String[] METHOD_BODY = new String[]{"public void ", "",
            "(", "", " context ){super.", "", "(context);}"};

    /**
     * 指定的类池对象
     */
    private final static ClassPool POOL;

    static {
        //初始化Map和Reduce两个属性
        HADOOP_MAPPER_CLASS = ReflectUtils.getClassObject(HadoopParam.HADOOP_MAPPER_CLASS_NAME);
        HADOOP_REDUCER_CLASS = ReflectUtils.getClassObject(HadoopParam.HADOOP_REDUCER_CLASS_NAME);

        //获取类池对象
        POOL = AssistTools.getClassPool();
    }

    public Provider() {
    }

    private static class InnerProvider {
        private static final Provider PROVIDER = new Provider();
    }

    /**
     * 获取当前类的实例对象
     *
     * @return
     */
    static Provider getInstance() {
        return InnerProvider.PROVIDER;
    }

    /**
     * 获取Mapper方法的map方法的方法体
     *
     * @return
     */
    String getMapBody(Type type) {
        StringBuilder sb = new StringBuilder();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("construct Map method body");
        }

        //拼接map方法体
        sb.append("public void map(").append(HadoopParam.HADOOP_LONGWRITABLE_CLASS).append(" key , ").
                append(((Class<?>) type).getName()).append(" value , ").
                append(HadoopParam.HADOOP_MAPPER_CLASS_NAME).append(".Context context").
                append("){super.map(key,value,context);}");

        return sb.toString();
    }

    /**
     * 获取Reduce子类的实例对象
     *
     * @param type
     */
    String getReduceBody(Type type) {
        StringBuilder sb = new StringBuilder();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("construct Reduce method body.");
        }

        //拼接子类实现的方法体
        sb.append("public void reduce(").append(((Class<?>) type).getName()).append(" key,").
                append("Iterable values,").append(HadoopParam.HADOOP_REDUCER_CLASS_NAME).append(".Context context").
                append("){super.reduce(key,values,context);}");
        return sb.toString();
    }

    /**
     * 构造Mapreduce方法类的子类,方便调用
     *
     * @param clazz
     * @param writeStrategy
     * @param partition
     * @return
     */
    CtClass createCtClass(Class<?> clazz, WriteStrategy writeStrategy, int partition) {

        if (!ReflectUtils.isSubClass(clazz, HADOOP_MAPPER_CLASS) && !ReflectUtils.isSubClass(clazz, HADOOP_REDUCER_CLASS)) {
            LOGGER.error("only create sub class of Hadoop Mapper or Reducer");
            throw new RuntimeException("only create sub class of Hadoop Mapper or Reducer");
        }
        //指定当前
        CtClass ctClass = POOL.makeClass(HadoopParam.SUB_CLASS_NAME + clazz.getSimpleName() + HadoopParam.SUB_CLASS_NAME_SUFFIX);

        try {
            //将当前的Class对象设置为生成类的父类
            CtClass superClass = POOL.get(clazz.getName());
            ctClass.setSuperclass(superClass);
        } catch (NotFoundException e) {
            LOGGER.error("can not find Class " + clazz.getName() + "turn to CtClass");
            throw new RuntimeException("can not find Class " + clazz.getName() + "turn to CtClass");
        } catch (CannotCompileException e) {
            e.printStackTrace();
        }

        String body = null;

        Type[] types = null;

        if (ReflectUtils.isSubClass(clazz, HADOOP_MAPPER_CLASS)) {
            types = ReflectUtils.getSuperGenericTypes(clazz, HadoopParam.HADOOP_MAPPER_CLASS_NAME);
        } else if (ReflectUtils.isSubClass(clazz, HADOOP_REDUCER_CLASS)) {
            types = ReflectUtils.getSuperGenericTypes(clazz, HadoopParam.HADOOP_REDUCER_CLASS_NAME);
        }

        if (null == types || types.length == 0) {
            throw new RuntimeException("can not obtain the class " + clazz.getName() + " parent's generic parameters");
        }

        //如果是Mapper的子类
        if (ReflectUtils.isSubClass(clazz, HADOOP_MAPPER_CLASS)) {
            body = getMapBody(types[1]);

            //添加setup方法到类结构上
            addMethodByArray(ctClass, METHOD_BODY, HadoopParam.SETUP_METHOD,
                    HadoopParam.HADOOP_MAPPER_CONTEXT_CLASS_NAME);

            //添加cleanup到类结构上
            addMethodByArray(ctClass, METHOD_BODY, HadoopParam.CLEANUP_METHOD,
                    HadoopParam.HADOOP_MAPPER_CONTEXT_CLASS_NAME);

        } else if (ReflectUtils.isSubClass(clazz, HADOOP_REDUCER_CLASS)) {
            //如果是Reducer的子类
            body = getReduceBody(types[1]);

            //添加setup方法到类结构上
            addMethodByArray(ctClass, METHOD_BODY, HadoopParam.SETUP_METHOD,
                    HadoopParam.HADOOP_REDUCER_CONTEXT_CLASS_NAME);

            //添加cleanup到类结构上
            addMethodByArray(ctClass, METHOD_BODY, HadoopParam.CLEANUP_METHOD,
                    HadoopParam.HADOOP_REDUCER_CONTEXT_CLASS_NAME);

        }

        //添加实现调用父类的重写方法
        addMethod(ctClass, body);

        //返回当前CtClass实例
        return ctClass;
    }

    /**
     * 获取最终真实生成的子类对象
     *
     * @param clazz
     * @return
     */
    Class<?> createClass(Class<?> clazz, WriteStrategy writeStrategy, int partition) {

        //保证partition符合分区规则
        partition = checkPartition(partition);

        //创建最终对应的CtClass对象
        CtClass ctClass = createCtClass(clazz, writeStrategy, partition);

        Class<?> realClass = null;

        try {

            //转换成Class对象返回
            realClass = ctClass.toClass();
        } catch (CannotCompileException e) {
            e.printStackTrace();
        }
        return realClass;
    }

    /**
     * 将对应方法体添加到对应的CtClass上
     *
     * @param ctClass
     * @param methodBody
     */
    private void addMethod(CtClass ctClass, String methodBody) {
        try {
            //根据方法内容添加方法,写入到指定的CtClass对象中
            CtMethod method = CtMethod.make(methodBody, ctClass);
            ctClass.addMethod(method);
        } catch (CannotCompileException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            LOGGER.error(methodBody);
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 添加对应的公开方法到对应的类结构上
     *
     * @param ctClass     指定的类结构
     * @param array       指定的方法字符串数组
     * @param methodName  指定的方法名称
     * @param contextName 执行的上下文名称
     */
    private void addMethodByArray(CtClass ctClass, String[] array, String methodName,
                                  String contextName) {

        //创建拼接数据对象
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < array.length; i++) {

            if (i == 1 || i == 5) {

                //索引位置为1或者5当前为方法名
                builder.append(methodName);
            } else if (i == 3) {

                //索引位置为3,当前为上下文的全限定名称
                builder.append(contextName);
            } else {

                //否则依次拼接数据
                builder.append(array[i]);
            }
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("construct method body : " + builder.toString());
        }

        //将该方法加到类结构上
        addMethod(ctClass, builder.toString());
    }

    /**
     * 校验当前的partition是否满足条件
     *
     * @param partition 当前的partition的分区数
     * @return partition的分区数
     */
    private static int checkPartition(int partition) {
        if (partition < MRConfig.DEFAULT_PARTITION) {
            partition = MRConfig.DEFAULT_PARTITION;
        }

        if (partition > MRConfig.MAX_PARTITION) {
            partition = MRConfig.MAX_PARTITION;
        }

        return partition;
    }
}
