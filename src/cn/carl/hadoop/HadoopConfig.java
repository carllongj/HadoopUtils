package cn.carl.hadoop;

/**
 * <p>Title: cn.carl.hadoop HadoopConfig</p>
 * <p>Description:
 * 注:如果使用自定义的Hadoop的配置文件,那么需要保证
 *     @see MRConfig 初始化之后,再使用此类
 * </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/23 20:48
 * @Version 1.0
 */
public class HadoopConfig {
    /**
     * 当前核心的CORE的配置文件
     */
    public static String CORE_SITE = MRConfig.CORE_SITE_XML_PATH;

    /**
     * 当前的文件系统配置文件
     */
    public static String HDFS_SITE = MRConfig.HDFS_SITE_XML_PATH;

    /**
     * 当前YARN的配置文件
     */
    public static String YARN_SITE = MRConfig.YARN_SITE_XML_PATH;

    /**
     * 当前MAPRED的配置文件
     */
    public static String MAPRED_SITE = MRConfig.MAPRED_SITE_XML_PATH;
}
