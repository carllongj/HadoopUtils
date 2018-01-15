package cn.carl.hadoop.wrapper;

import cn.carl.io.resource.ResourceTools;
import cn.carl.io.resource.input.RealResource;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * <p>Title: cn.carl.hadoop.wrapper ConfigurationWrapper</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/30 19:25
 * @Version 1.0
 */
public class ConfigurationWrapper extends Configuration {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(ConfigurationWrapper.class);

    /**
     * 通过文件夹来创建一个输入配置文件的路径
     * <p>
     * 一个文件输入路径的构造器,通过这个文件路径,寻找其路径下的所有xml配置文件
     *
     * @param path 一个文件夹路径
     */
    public ConfigurationWrapper(String path) {

        super();

        //创建当前的文件对象
        File file = new File(path);

        //当前的指定的文件对存并且当前文件为目录
        if (file.exists() && file.isDirectory()) {

            //获取该目录下的所有文件
            File[] files = file.listFiles();
            if (null != files && files.length > 0) {

                //遍历当前目录下的所有文件
                for (File realFile : files) {

                    //判断当前文件是否为文件且后缀为xml文件的才处理
                    if (realFile.isFile() &&
                            realFile.getName().endsWith(".xml")) {

                        //获取当前的资源文件
                        RealResource resource = ResourceTools.
                                getRealResource(realFile.getAbsolutePath());

                        try {
                            super.addResource(resource.getInputStream());
                        } catch (IOException e) {
                            LOGGER.error("can not read the inputPath " + realFile.getAbsolutePath());
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    /**
     * 通过指定数组路径来创建对应的对象
     *
     * @param paths 输入数据对象
     */
    public ConfigurationWrapper(String[] paths) {

        //输入的数据路径不能为null
        if (paths != null) {

            //遍历每一个输入数据的对象
            for (String path : paths) {

                //获取该资源数据
                RealResource realResource = ResourceTools.getRealResource(path);

                try {

                    //添加资源
                    super.addResource(realResource.getInputStream());

                } catch (IOException e) {
                    LOGGER.error("can not read the inputPath " + path);
                    e.printStackTrace();
                }
            }
        }
    }
}
