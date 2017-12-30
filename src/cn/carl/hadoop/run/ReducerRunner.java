package cn.carl.hadoop.run;

import cn.carl.hadoop.HadoopParam;
import cn.carl.hadoop.MRConfig;
import cn.carl.hadoop.config.ContextConfig;
import cn.carl.io.CloseTools;
import cn.carl.io.IOTools;
import cn.carl.string.StringTools;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Reducer任务的运行器类
 * <p>
 * <p>Title: cn.carl.hadoop.run ReducerRunner</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/28 21:19
 * @Version 1.0
 */
public class ReducerRunner extends AbstractRunner {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(ReducerRunner.class);

    /**
     * 获取当前执行reduce方法名称
     */
    private static final String REDUCE_METHOD = "reduce";

    /**
     * 当前执行任务的
     */
    private final ReducerParam reducerParam;

    /**
     * 获取当前reduce的键的类型
     */
    private final Class<?> keyClass;

    /**
     * 获取当前reduce的集合的类型
     */
    private final Class<?> valueClass;

    /**
     * 当前数据的映射对象
     */
    protected final Map<Integer, Map> dataMap = new HashMap<>();


    public ReducerRunner(Class<?> mrClass, ContextConfig contextConfig) {
        super(mrClass, contextConfig);

        //获取当前的Reducer的分区数据
        RunnerParam runnerParam = this.contextConfig.getRunnerParam();

        //如果当前是ReducerParam类,则强制转换保存
        if (runnerParam instanceof ReducerParam) {
            this.reducerParam = (ReducerParam) runnerParam;
        } else {

            //否则构造一个默认的参数对象
            this.reducerParam = new ReducerParam(runnerParam, MRConfig.DEFAULT_PARTITION,
                    ReducerParam.DEFAULT_PARTITION, ReducerParam.DEFAULT_COMPARATOR);
        }

        //获取当前的key的类型
        this.keyClass = (Class<?>) this.contextConfig.getGenericParameters()[0];

        //获取当前的value的类型
        this.valueClass = (Class<?>) this.contextConfig.getGenericParameters()[1];
    }

    /**
     * 运行mapreduce运行的入口类
     *
     * @return 执行的结果, true为成功, false为失败
     */
    @Override
    public final boolean runHadoop() {

        LOGGER.info("prepare execute the class of Reducer");

        //当前任务执行的类
        final Object instance = this.mrInstance;

        String currentPath;


        //如果指定了初始化方法名,只执行初始化方法
        if (StringTools.hasText(this.contextConfig.getRunnerParam().getInitMethod())) {

            LOGGER.info("execute init method " + this.contextConfig.getRunnerParam().getInitMethod());

            invokeMethod(this.contextConfig.getRunnerParam().getInitMethod(), instance);
        }

        LOGGER.info("prepare hash the data to the map");

        while (null != (currentPath = this.getNextFile())) {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("current path is " + currentPath);
            }

            try {

                //执行核心的数据分区方法
                invokeReduce(currentPath);

            } catch (Exception e) {
                LOGGER.error("can not execute Class " + mrClass.getName());
                throw new RuntimeException(e.getMessage());
            }
        }


        LOGGER.info("execute the specific class of reducer");

        doIterator(dataMap);

        LOGGER.info("reduce method is execute finished");

        //调用执行该类父类的回调方法
        if (StringTools.hasText(this.contextConfig.getRunnerParam().getCallback())) {

            LOGGER.info("execute callback method " + this.contextConfig.getRunnerParam().getCallback());
            invokeMethod(this.contextConfig.getRunnerParam().getCallback(), instance);
        }

        LOGGER.info("reduce execute successfully");
        return true;
    }

    /**
     * 对数据进行迭代处理的方法
     *
     * @param dataMap 数据集合
     */
    private void doIterator(Map<Integer, Map> dataMap) {

        final Object instance = this.mrInstance;

        Method method;

        try {
            Class<?> keyClass = (Class<?>) this.contextConfig.getGenericParameters()[0];

            //获取当前的reduce的方法
            method = mrClass.getDeclaredMethod(REDUCE_METHOD,
                    keyClass, Iterable.class, Reducer.Context.class);

        } catch (NoSuchMethodException e) {
            LOGGER.error("can not find method " + REDUCE_METHOD);
            throw new RuntimeException(e.getMessage());
        }

        //遍历每一个分区的数据
        for (Map.Entry<Integer, Map> entry : dataMap.entrySet()) {

            Iterator iterator = entry.getValue().entrySet().iterator();

            //遍历一个分区下每一个分组的数据
            while (iterator.hasNext()) {

                //获取当前分组的数据
                Map.Entry nextEntry = (Map.Entry) iterator.next();

                Object key = nextEntry.getKey();

                Object value = nextEntry.getValue();

                //如果需要执行初始化方法,执行初始化方法
                if (this.contextConfig.getRunnerParam().isExecuteSetup()) {
                    invokeContextMethod(HadoopParam.SETUP_METHOD, instance);
                }

                try {
                    method.invoke(instance, key, value, this.contextConfig.getContext());
                } catch (ReflectiveOperationException e) {
                    LOGGER.error("can not invoke the method " + method.getName());
                }

                //调用清空数据的方法
                if (this.contextConfig.getRunnerParam().isExecuteCleanup()) {
                    invokeContextMethod(HadoopParam.CLEANUP_METHOD, instance);
                }
            }
        }
    }

    /**
     * 指定当前指定的清空数据的方法
     *
     * @param methodName 方法名称
     * @param instance   该运行类的对象
     */
    private void invokeMethod(String methodName, Object instance) {
        try {
            Method method = instance.getClass().getDeclaredMethod(methodName, Reducer.Context.class);

            //执行当前的指定名称方法
            if (null != method) {
                method.invoke(instance, this.contextConfig.getContext());
            }

        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not invoke the method named " + methodName);
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * 执行该任务的上下文方法
     *
     * @param methodName
     * @param instance
     */
    private void invokeContextMethod(String methodName, Object instance) {
        try {
            Method method = instance.getClass().getDeclaredMethod(methodName, Reducer.Context.class);

            //执行当前的指定名称方法
            if (null != method) {
                method.invoke(instance, this.contextConfig.getContext());
            }

        } catch (ReflectiveOperationException e) {
            LOGGER.error("can not invoke the method named " + methodName);
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * 对当前的数据进行分区处理
     *
     * @param path 当前运行的任务路径
     */
    private void invokeReduce(String path) {

        BufferedReader reader = IOTools.createReader(path);

        if (null == reader) {
            throw new RuntimeException("can not read file " + path);
        }

        try {
            String dataInput;

            while (null != (dataInput = reader.readLine())) {
                //对输入的数据进行分区处理
                dataPartition(this.reducerParam, dataInput);
            }

        } catch (Exception e) {
            LOGGER.error("can not execute Reduce Class " + mrClass.getName());
            throw new RuntimeException("can not execute Reduce Class " + mrClass.getName());
        } finally {
            CloseTools.closeStream(reader);
        }
    }

    /**
     * 执行对数据进行分组处理
     *
     * @param reducerParam 当前reduce任务的处理
     * @param input        当前读入的一条数据
     */
    private void dataPartition(ReducerParam reducerParam, String input) {

        //定义当前切分数据的划分长度
        int dataLength = 2;

        //获取当前切分map输出数据的分隔符
        String splitValue = this.contextConfig.getWriteStrategy().getSplitValue();

        //获取结果数据
        String[] result = input.split(splitValue);

        //获取分区的类
        Partitioner partitioner = reducerParam.getPartitionClass();

        //获取分组的类
        WritableComparator comparator = reducerParam.getGroupComparatorClass();

        //当前长度为2才处理当前的数据
        if (result.length == dataLength) {

            //获取当前key和value的数据
            String key = result[0];
            String value = result[1];

            Object realkey = getKeyValue(key, this.keyClass);
            Object realValue = getKeyValue(value, this.valueClass);

            if (null == realkey || null == realValue) {
                return;
            }

            //将数据进行路由处理
            dataRoute(realkey, realValue, partitioner, comparator);
        }
    }

    /**
     * 通过获取键和值的类型来构造对应的数据类型
     *
     * @param data  真实的输入数据
     * @param clazz 指定的类型
     * @return 当前的键值对象
     */
    protected Object getKeyValue(String data, Class<?> clazz) {

        //指定的方法名称
        String setMethodName = "set";

        try {

            //创建当前的实例对象
            Object newInstance = clazz.newInstance();

            //获取设置数据的方法
            Method method = clazz.getDeclaredMethod(setMethodName, String.class);

            //将值设置到当前对象数据中
            method.invoke(newInstance, data);

            //返回当前的数据对象
            return newInstance;

        } catch (ReflectiveOperationException e) {
            e.printStackTrace();
            LOGGER.error("can not create data of class " + clazz.getName());
        }

        return null;
    }

    /**
     * 将对应的数据进行路由
     *
     * @param key                指定的key的数据
     * @param value              指定的Value的数据
     * @param partition          当前分区数据的对象
     * @param writableComparator 当前比较器的对象
     */
    protected void dataRoute(Object key, Object value, Partitioner partition,
                             WritableComparator writableComparator) {

        Integer realPartition = partition.getPartition(key, value, this.reducerParam.getPartition());

        if (dataMap.containsKey(realPartition)) {
            TreeMapList dataCollection = (TreeMapList) dataMap.get(realPartition);

            dataCollection.putKeyValue(key, value);
        } else {

            TreeMapList dataCollection = new TreeMapList(writableComparator);

            dataCollection.putKeyValue(key, value);

            dataMap.put(realPartition, dataCollection);
        }
    }

    /**
     * 静态内部类保存对应的数据集合
     *
     * @param <K> 保存的Key的数据
     * @param <V> 保存的Value的数据
     */
    private static class TreeMapList<K, V> extends TreeMap<K, List<V>> {

        public TreeMapList(Comparator<? super K> comparator) {
            super(comparator);
        }

        /**
         * 存入当前的数据到分区汲取数据中
         *
         * @param key   当前的Key数据
         * @param value 当前的Value数据
         */
        public void putKeyValue(K key, V value) {

            //如果当前数据取包含了当前的键的数据,取出集合,将值加进去
            if (this.containsKey(key)) {
                this.get(key).add(value);
            } else {
                //否则,创加一个新的集合,将数据加进去
                List<V> dataList = new ArrayList<>();
                this.put(key, dataList);

                dataList.add(value);
            }
        }
    }
}
