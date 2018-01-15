package cn.carl.hadoop.mr;

import cn.carl.hadoop.config.ContextConfig;
import cn.carl.hadoop.wrapper.FileSplitWrapper;
import cn.carl.other.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * <p>Title: cn.carl.hadoop.mr DefaultMapper</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 20:54
 * @Version 1.0
 */
public class DefaultMapper extends DefaultMapperWrapper {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(DefaultMapper.class);

    /**
     * 获取默认指定的Mapper的Context对象
     *
     * @return
     */
    public Mapper.Context getContext(ContextConfig contextConfig) {
        Assert.assertNotNull(contextConfig);

        //所有的配置在该对象中

        return new DefaultMapperContext(contextConfig);
    }

    class DefaultMapperContext extends DefaultMapperWrapper.MapperWrapperContext implements WriteStrategyHolder {

        private final ContextConfig contextConfig;

        /**
         * 构造一个处理write方法的策略对象
         *
         * @param
         */
        public DefaultMapperContext(ContextConfig contextConfig) {
            this.contextConfig = contextConfig;
        }

        @Override
        public Counter getCounter(String s, String s1) {
            return null;
        }

        @Override
        public void write(Object o, Object o2) throws IOException, InterruptedException {

            //获取该配置的数据写出方式
            WriteStrategy writeStrategy = this.contextConfig.getWriteStrategy();

            if (null != writeStrategy) {
                writeStrategy.write(o, o2);
            } else {
                throw new RuntimeException("WriteStrategy can not be null");
            }
        }

        @Override
        public WriteStrategy getWriteStrategy() {
            return this.contextConfig.getWriteStrategy();
        }

        /**
         * 获取输入数据的路径
         *
         * @return 输入数据的路径
         */
        @Override
        public InputSplit getInputSplit() {
            return (InputSplit) this.contextConfig.getFileSplitWrapper().
                    getWrapperInstance();
        }

        /**
         * 获取当前的配置对象
         *
         * @return 获取任务运行的配置对象
         */
        @Override
        public Configuration getConfiguration() {
            return this.contextConfig.getConfiguration();
        }
    }
}
