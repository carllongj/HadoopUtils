package cn.carl.hadoop.mr;

import cn.carl.hadoop.config.ContextConfig;
import cn.carl.other.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <p>Title: cn.carl.hadoop.mr DefaultReducer</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 21:11
 * @Version 1.0
 */
public class DefaultReducer extends DefaultReducerWrapper {

    static class InnerReducer {
        private static final DefaultReducer CONTEXT = new DefaultReducer();
    }

    /**
     * 获取单例模式
     *
     * @return
     */
    public static DefaultReducer getInstance() {
        return InnerReducer.CONTEXT;
    }

    /**
     * 获取Reducer类的上下文对象
     *
     * @param contextConfig 任务运行的配置对性爱那个
     * @return
     */
    public Reducer.Context getContext(ContextConfig contextConfig) {
        Assert.assertNotNull(contextConfig);
        return new DefaultReducerContext(contextConfig);
    }

    /**
     * 默认的Reduce的上下文对象
     */
    class DefaultReducerContext extends DefaultReducerWrapper.ReducerContextWrapper implements WriteStrategyHolder {

        private final ContextConfig contextConfig;

        public DefaultReducerContext(ContextConfig contextConfig) {
            this.contextConfig = contextConfig;
        }

        @Override
        public void write(Object o, Object o2) throws IOException, InterruptedException {

            WriteStrategy writeStrategy = this.contextConfig.getWriteStrategy();

            writeStrategy.write(o, o2);
        }

        @Override
        public Counter getCounter(String s, String s1) {
            return super.getCounter(s, s1);
        }

        /**
         * 写出数据的策略
         *
         * @return 当前的写出策略方式
         */
        @Override
        public WriteStrategy getWriteStrategy() {
            return this.contextConfig.getWriteStrategy();
        }

        /**
         * 获取当前任务运行的配置对象
         *
         * @return 返回当前的配置对象
         */
        @Override
        public Configuration getConfiguration() {
            return this.contextConfig.getConfiguration();
        }
    }
}
