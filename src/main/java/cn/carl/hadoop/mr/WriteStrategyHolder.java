package cn.carl.hadoop.mr;

/**
 * <p>Title: cn.carl.hadoop.mr ContextHolder</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/22 18:32
 * @Version 1.0
 */
public interface WriteStrategyHolder {
    /**
     * 获取当前Context对象中的写的策略对象
     * @return
     */
    WriteStrategy getWriteStrategy();
}
