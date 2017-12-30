package cn.carl.hadoop.run;

import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Logger;

/**
 * Reducer对于某些参数有写特殊需求
 * <p>Title: cn.carl.hadoop.run ReducerParam</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/28 21:38
 * @Version 1.0
 */
public class ReducerParam extends RunnerParam {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(ReducerParam.class);

    /**
     * 默认分区的全限定名称
     */
    public static final String DEFAULT_PARTITION = "DefaultPartitioner";

    /**
     * 默认的分组类的全限定名称
     */
    public static final String DEFAULT_COMPARATOR = "DefaultComparator";

    /**
     * 获取当前对reduce处理的数据分区
     * 注:此字段对Mapper无效,只对reduce有效
     */
    private int partition;

    /**
     * 只对Reducer类处理
     * reduce对当前输出的KEY的数据进行分区,相同分
     * 区的数据保存在一个数据区.
     */
    private String partitionClassName;

    /**
     * 当前分区对象
     */
    private Partitioner partitionClass;

    /**
     * 对分区分组的数据进行处理的全限定名称
     */
    private String groupComparatorClassName;


    /**
     * 当前分组对象
     */
    private WritableComparator groupComparatorClass;


    /**
     * 指定分区数,分区类名,和分组全限定名称
     *
     * @param partition                分区数->指定的是reducer的数量
     * @param partitionClassName       分区类的全限定名称
     * @param groupComparatorClassName 分组的全限定名
     */
    public ReducerParam(int partition, String partitionClassName, String groupComparatorClassName) {
        this.partition = partition;
        this.partitionClassName = partitionClassName;
        this.groupComparatorClassName = groupComparatorClassName;

        //初始化对应的分区,分组的对象
        init();
    }

    /**
     * 提供默认的类构造器的分区分组数据信息
     *
     * @param runnerParam              非ReduceParam的对象
     * @param partition                分区的大小,默认指定为1
     * @param partitionClassName       分区类的全限定名称
     * @param groupComparatorClassName 分组类的全限定名称
     */
    ReducerParam(RunnerParam runnerParam, int partition, String partitionClassName,
                 String groupComparatorClassName) {
        this(partition, partitionClassName, groupComparatorClassName);

        //拷贝指定的属性到该类中
        this.executeCleanup = runnerParam.isExecuteCleanup();
        this.executeSetup = runnerParam.isExecuteSetup();
        this.initMethod = runnerParam.getInitMethod();
        this.callback = runnerParam.getCallback();
    }

    public int getPartition() {
        return partition;
    }

    public void setPartition(int partition) {
        this.partition = partition;
    }

    public String getPartitionClassName() {
        return partitionClassName;
    }

    public void setPartitionClassName(String partitionClassName) {
        this.partitionClassName = partitionClassName;
    }

    public Partitioner getPartitionClass() {
        return partitionClass;
    }

    public void setPartitionClass(Partitioner partitionClass) {
        this.partitionClass = partitionClass;
    }

    public String getGroupComparatorClassName() {
        return groupComparatorClassName;
    }

    public void setGroupComparatorClassName(String groupComparatorClassName) {
        this.groupComparatorClassName = groupComparatorClassName;
    }

    public WritableComparator getGroupComparatorClass() {
        return groupComparatorClass;
    }

    public void setGroupComparatorClass(WritableComparator groupComparatorClass) {
        this.groupComparatorClass = groupComparatorClass;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        ReducerParam that = (ReducerParam) o;

        if (partition != that.partition) {
            return false;
        }
        if (partitionClassName != null ? !partitionClassName.equals(that.partitionClassName) : that.partitionClassName != null) {
            return false;
        }
        if (partitionClass != null ? !partitionClass.equals(that.partitionClass) : that.partitionClass != null) {
            return false;
        }
        if (groupComparatorClassName != null ? !groupComparatorClassName.equals(that.groupComparatorClassName) : that.groupComparatorClassName != null) {
            return false;
        }

        return groupComparatorClass != null ? groupComparatorClass.equals(that.groupComparatorClass) : that.groupComparatorClass == null;

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + partition;
        result = 31 * result + (partitionClassName != null ? partitionClassName.hashCode() : 0);
        result = 31 * result + (partitionClass != null ? partitionClass.hashCode() : 0);
        result = 31 * result + (groupComparatorClassName != null ? groupComparatorClassName.hashCode() : 0);
        result = 31 * result + (groupComparatorClass != null ? groupComparatorClass.hashCode() : 0);
        return result;
    }

    private void init() {
        try {
            //创建当前分区类的实例
            this.partitionClass = (Partitioner)
                    (Class.forName(partitionClassName).newInstance());
        } catch (ReflectiveOperationException e) {
            LOGGER.warn("can not execute partition class " + partitionClassName + " use default instead");
            try {
                this.partitionClass = (Partitioner)
                        (Class.forName(DEFAULT_PARTITION).newInstance());
            } catch (ReflectiveOperationException e1) {
                LOGGER.error("can not execute partition class " + DEFAULT_PARTITION);
            }
        }

        try {
            this.groupComparatorClass = (WritableComparator)
                    Class.forName(groupComparatorClassName).newInstance();
        } catch (ReflectiveOperationException e) {
            LOGGER.warn("can not execute comparator class named " + partitionClassName + " use default instead");
            try {
                this.groupComparatorClass = (WritableComparator)
                        Class.forName(DEFAULT_COMPARATOR).newInstance();
            } catch (ReflectiveOperationException e1) {
                LOGGER.error("can not execute default class " + DEFAULT_COMPARATOR);
            }
        }
    }
}
