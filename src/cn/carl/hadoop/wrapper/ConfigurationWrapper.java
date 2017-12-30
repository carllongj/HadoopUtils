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

    public ConfigurationWrapper(String path) {

        super();

        //创建当前的文件对象
        File file = new File(path);

        //当前的指定的文件对存并且当前文件为目录
        if (file.exists() && file.isDirectory()) {

            //获取该目录下的所有文件
            File[] files = file.listFiles();
            if (files.length > 0) {

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

    public ConfigurationWrapper(String[] paths) {
        
        if (null == paths) {
            throw new RuntimeException("input path can not be null");
        }

        for (String path : paths) {
            RealResource realResource = ResourceTools.getRealResource(path);

            try {
                super.addResource(realResource.getInputStream());
            } catch (IOException e) {
                LOGGER.error("can not read the inputPath " + path);
                e.printStackTrace();
            }
        }
    }
}
