package cn.carl.hadoop.wrapper;

import cn.carl.hadoop.config.ContextConfig;
import cn.carl.hadoop.mr.FileWrite;
import cn.carl.hadoop.mr.WriteStrategy;
import cn.carl.io.file.FileTools;
import cn.carl.string.StringTools;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

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
     * 一个默认的输出数据路径
     */
    private static final String TEMP_PATH = System.getProperty("user.dir") +
            File.separator + "temp_run";

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
        this.write(namedOutput, key, value, workspacePath);
    }

    @Override
    public <K1, V1> void write(String namedOutput, K1 key, V1 value, String baseOutputPath)
            throws IOException, InterruptedException {
        String fileKey = baseOutputPath + namedOutput;

        //获取当前的通道对象
        WriteStrategy writeStrategy = getWriteStrategy(fileKey, namedOutput, this.namedOutput);

        //写出数据
        writeStrategy.write(key, value);
    }

    @Override
    public void write(K key, V value, String baseOutputPath)
            throws IOException, InterruptedException {


        //获取当前路径的输出数据
        WriteStrategy strategy = getWriteStrategy(baseOutputPath, null, baseNamedOutput);

        //输出数据
        strategy.write(key, value);
    }

    /**
     * 获取当前写出策略的方法
     *
     * @param baseOutputPath 数据路径的key数据
     * @param namedPath      输出的文件名称
     * @param outputs        输出管道
     * @return
     */
    private WriteStrategy getWriteStrategy(String baseOutputPath, String namedPath,
                                           Map<String, WriteStrategy> outputs) {

        WriteStrategy fileWrite = null;

        if (null == outputs.get(baseOutputPath)) {


            //获取当前的输出路径
            String realPath = null;

            if (StringTools.hasText(namedPath) && StringTools.hasText(baseOutputPath)) {

                //当前有输出数据路径
                realPath = baseOutputPath + namedPath;

            } else if (!StringTools.hasText(baseOutputPath) || this.workspacePath.equals(baseOutputPath)) {
                //声明当前的文件
                File file;

                //如果改路径为空或者是和指定输出路径冲突,则重新指定默认路径作为输出
                //创建该文件对象
                file = new File(baseOutputPath);

                //如果当前文件存在或者文件是一个文件夹
                if (file.exists() || file.isDirectory()) {

                    //重新创建一个路径,使用默认的路径
                    file = new File(TEMP_PATH);

                    //如果还是存在
                    if (file.exists() || file.isDirectory()) {

                        //强制删除这个文件或者文件夹
                        FileTools.forceDeleteFile(file);
                        realPath = file.getPath();
                    }
                }
            }

            //创建改路径下的数据文件
            fileWrite = new FileWrite(realPath);

            //将数据输出放到该映射集合中
            outputs.put(realPath, fileWrite);

        } else {

            //否则获取该通道
            outputs.get(baseOutputPath);
        }

        return fileWrite;
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
