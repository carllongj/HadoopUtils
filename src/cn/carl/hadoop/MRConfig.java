package cn.carl.hadoop;

/**
 * <p>Title: cn.carl.hadoop MRConfig</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/22 18:45
 * @Version 1.0
 */
public class MRConfig {

    public static String CORE_SITE_XML_PATH = "/file/framework/hadoop/core-site.xml";

    public static String HDFS_SITE_XML_PATH = "/file/framework/hadoop/hdfs-site.xml";

    public static String YARN_SITE_XML_PATH = "/file/framework/hadoop/mapred-site.xml";

    public static String MAPRED_SITE_XML_PATH = "/file/framework/hadoop/yarn-site.xml";

    /**
     * 执行Reducer的最大的分区数
     */
    public static final int MAX_PARTITION = 16;

    /**
     * 执行Reducer的最小的分区数
     */
    public static final int DEFAULT_PARTITION = 1;

    /**
     * 设置Hadoop的核心配置文件
     *
     * @param path 核心配置文件的路径
     */
    public static void setCoreSiteXmlPath(String path) {
        CORE_SITE_XML_PATH = path;
    }

    /**
     * 设置Hadoop文件系统的配置文件
     *
     * @param path hdfs文件系统的配置路径
     */
    public static void setHdfsSiteXmlPath(String path) {
        HDFS_SITE_XML_PATH = path;
    }

    /**
     * 设置Hadoop Yarn的配置文件
     *
     * @param path yarn的配置文件
     */
    public static void setYarnSiteXmlPath(String path) {
        YARN_SITE_XML_PATH = path;
    }

    /**
     * 设置Hadoop MapReducer 的配置文件
     *
     * @param path
     */
    public static void setMapredSiteXmlPath(String path) {
        MAPRED_SITE_XML_PATH = path;
    }


}
