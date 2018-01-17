package cn.carl.hadoop.wrapper;

import cn.carl.hadoop.config.ContextConfig;
import cn.carl.hadoop.mr.FileWrite;
import cn.carl.hadoop.mr.WriteStrategy;
import cn.carl.io.file.FileTools;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>Title: cn.carl.hadoop.wrapper MultipleOutputs</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2018/1/15 18:49
 * @Version 1.0
 */
public class MultipleOutputsWrapper<K, V> extends MultipleOutputs<K, V> {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(MultipleOutputsWrapper.class);

    /**
     * 设置当前的输出路径的根路径
     */
    private final String workspacePath;

    /**
     * 对应数据的指定名称的输出数据
     */
    private Map<String, WriteStrategy> namedOutput = new ConcurrentHashMap<>();

    /**
     * 对应绝对路径的输出数据路径的集合
     */
    private Map<String, WriteStrategy> baseNamedOutput = new ConcurrentHashMap<>();

    /**
     * 构造一个多路径输出的对象
     *
     * @param contextConfig 全局配置对象
     */
    public MultipleOutputsWrapper(ContextConfig contextConfig) {
        super(contextConfig.getContext());

        //获取当前的输出数据路径
        this.workspacePath = contextConfig.getOutputPath();
    }

    /**
     * 输出数据到指定的路径下
     *
     * @param namedOutput 指定一个名称作为文件名,具有相同的namedOutput数据是输出到一个文件中
     * @param key         输出的key的数据
     * @param value       输出的value的数据
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public <K1, V1> void write(String namedOutput, K1 key, V1 value) throws IOException, InterruptedException {
        this.write(namedOutput, key, value, namedOutput);
    }

    /**
     * 指定一个路径输出
     *
     * @param namedOutput    指定名称的路径
     * @param key            输出的KEY数据
     * @param value          输出的VALUE数据
     * @param baseOutputPath 指定的文件名称
     * @param <K1>
     * @param <V1>
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public <K1, V1> void write(String namedOutput, K1 key, V1 value, String baseOutputPath)
            throws IOException, InterruptedException {

        String fileKey = FileTools.toPathString(this.workspacePath, baseOutputPath);

        //获取当前的通道对象
        WriteStrategy writeStrategy = getWriteStrategy(fileKey, namedOutput, this.namedOutput);

        //写出数据
        writeStrategy.write(key, value);
    }

    /**
     * 当前写出的数据方法,目前该方法不支持
     *
     * @param key            输出数据的KEY的数据
     * @param value          输出数据的VALUE数据
     * @param baseOutputPath 输出数据的路径
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void write(K key, V value, String baseOutputPath)
            throws IOException, InterruptedException {
        LOGGER.error("This method is not support");
        throw new UnsupportedOperationException("This Class" + this.getClass().getName()
                + " write(K,V,String) is not supported now");
    }

    /**
     * 获取当前写出策略的方法
     *
     * @param baseOutputPath 数据路径的key数据
     * @param namedPath      输出的文件名称
     * @param outputs        输出管道集合
     * @return 写出数据的对象
     */
    private WriteStrategy getWriteStrategy(String baseOutputPath, String namedPath,
                                           Map<String, WriteStrategy> outputs) {

        //声明一个写出策略方式
        WriteStrategy writeStrategy;

        //获取当前的输出数据
        String pathString = FileTools.toPathString(baseOutputPath, namedPath);

        if (outputs.containsKey(pathString)) {

            //获取上一次的输出数据通道
            writeStrategy = outputs.get(pathString);

        } else {

            //创建一个文件对象
            File file = new File(pathString);

            //当前如果存在此路径且此路径是一个文件夹,删除该文件和目录
            if (file.exists() && file.isDirectory()) {

                //强制递归删除该文件或目录
                FileTools.forceDeleteFile(file, true);
            }

            //获取当前的输出数据通道
            writeStrategy = new FileWrite(pathString);
        }

        return writeStrategy;
    }

    @Override
    public void close() throws IOException, InterruptedException {

        //遍历关闭该所有的路径资源
        for (Map.Entry<String, WriteStrategy> entry : baseNamedOutput.entrySet()) {
            entry.getValue().close();
        }

        //遍历关闭该所有的路径资源
        for (Map.Entry<String, WriteStrategy> entry : namedOutput.entrySet()) {
            entry.getValue().close();
        }
    }
}
