package com.carl.hadoop.run;

/**
 * Hadoop的运行器接口,任务的运行通过此类执行
 * <p>Title: cn.carl.hadoop.run</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 19:40
 * @Version 1.0
 */
public interface HadoopRunner {

    /**
     * 运行当前的任务,返回最后的执行结果
     * true ->执行成功
     * false->执行失败
     *
     * @return 返回当前执行任务的结果
     */
    boolean runHadoop();

    /**
     * 获取当前的下一个文件路径
     *
     * @return
     */
    String getNextFile();
}
