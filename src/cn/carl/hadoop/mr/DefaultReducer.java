package cn.carl.hadoop.mr;

import cn.carl.other.Assert;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * <p>Title: cn.carl.hadoop.mr DefaultReducer</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 21:11
 * @Version 1.0
 */
public class DefaultReducer extends DefaultReducerWrapper {

    static class InnerReduer{
        private static final DefaultReducer CONTEXT = new DefaultReducer();
    }

    /**
     * 获取单例模式
     * @return
     */
    public static DefaultReducer getInstance(){
        return InnerReduer.CONTEXT;
    }

    /**
     * 获取Reducer类的上下文对象
     * @param writeStrategy
     * @return
     */
    public Reducer.Context getContext(WriteStrategy writeStrategy) {
        Assert.assertNotNull(writeStrategy);
        return new DefaultReducerContext(writeStrategy);
    }

    class DefaultReducerContext extends DefaultReducerWrapper.ReducerContextWrapper implements WriteStrategyHolder{

        /** 写的方式的策略 */
        private final WriteStrategy writeStrategy;

        public DefaultReducerContext(WriteStrategy writeStrategy) {
            this.writeStrategy = writeStrategy;
        }

        @Override
        public void write(Object o, Object o2) throws IOException, InterruptedException {
            writeStrategy.write(o, o2);
        }

        @Override
        public Counter getCounter(String s, String s1) {
            return super.getCounter(s, s1);
        }

        @Override
        public WriteStrategy getWriteStrategy() {
            return writeStrategy;
        }
    }
}
