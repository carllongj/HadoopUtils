package com.carl.hadoop.mr;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Objects;

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
    public Mapper.Context getContext(WriteStrategy writeStrategy, FileSplitWrapper fileSplitWrapper) {
        Objects.requireNonNull(writeStrategy);
        return new DefaultMapperContext(writeStrategy, fileSplitWrapper);
    }

    class DefaultMapperContext extends DefaultMapperWrapper.MapperWrapperContext implements WriteStrategyHolder {

        /**
         * 保留策略处理mr的write方法
         */
        private final WriteStrategy writeStrategy;

        /**
         * 当前定义的切分数据的原始文件名称
         */
        private final FileSplitWrapper fileSplitWrapper;

        /**
         * 构造一个处理write方法的策略对象
         *
         * @param writeStrategy
         */
        public DefaultMapperContext(WriteStrategy writeStrategy, FileSplitWrapper fileSplitWrapper) {
            this.writeStrategy = writeStrategy;
            this.fileSplitWrapper = fileSplitWrapper;
        }

        @Override
        public Counter getCounter(String s, String s1) {
            return null;
        }

        @Override
        public void write(Object o, Object o2) throws IOException, InterruptedException {
            if (null != writeStrategy) {
                writeStrategy.write(o, o2);
            } else {
                throw new RuntimeException("WriteStrategy can not be null");
            }
        }

        @Override
        public WriteStrategy getWriteStrategy() {
            return writeStrategy;
        }

        @Override
        public InputSplit getInputSplit() {
            return (InputSplit) this.fileSplitWrapper.getWrapperInstance();
        }
    }
}
