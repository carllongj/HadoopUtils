package cn.carl.hadoop.mr;

import cn.carl.hadoop.HadoopParam;
import cn.carl.io.nio.BufferWriter;
import cn.carl.string.StringTools;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * 自定义的写出数据文件策略方式
 * <p>Title: cn.carl.hadoop.mr FileWrite</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/11/2 21:04
 * @Version 1.0
 */
public class FileWrite implements WriteStrategy {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(FileWrite.class);

    /**
     * 输出文件数据的通道
     */
    private FileChannel channel;

    /**
     * 当前的是否已经创建了channel对象
     */
    private boolean loaded;

    /**
     * 获取当前写出数据的路径
     */
    private String path;

    /**
     * 一个写出数据类带有该类的实例
     */
    private final BufferWriter BUFFER_WRITER = new BufferWriter();

    /**
     * 默认的文件类型输出构造器
     * 默认将文件写出到user.dir目录下
     */
    public FileWrite() {
        this(Paths.get(System.getProperty("user.dir")) + StringTools.SLASH +
                String.valueOf(System.currentTimeMillis()), false);
    }

    /**
     * 使用路径构造器,默认不创建文件
     *
     * @param path 创建文件的路径
     */
    public FileWrite(String path) {
        this(path, false);
    }

    /**
     * 根据指定的路径,是否创建文件
     *
     * @param path       指定的路径
     * @param createFile 是否创建对应的文件
     */
    public FileWrite(String path, boolean createFile) {
        this.path = path;

        if (createFile) {
            load();
        }
        this.loaded = createFile;
    }

    @Override
    public void write(Object o1, Object o2) {

        if (!loaded) {

            load();

            //设置当前已加载过,已经创建了文件
            loaded = true;
        }

        //写出数据
        if (o2 instanceof NullWritable) {

            //NullWritable默认不输出
            //添加换行符
            BUFFER_WRITER.writeChannel(o1.toString() + System.lineSeparator(), channel);
        } else {

            String out = o1.toString() + HadoopParam.KEY_VALUE_SPLIT_STANDARD +
                    o2.toString() + System.lineSeparator();
            BUFFER_WRITER.writeChannel(out, channel);
        }
    }

    @Override
    public String getSplitValue() {
        return HadoopParam.KEY_VALUE_SPLIT_STANDARD;
    }

    @Override
    public void close() throws IOException {
        this.channel.close();
    }

    /**
     * 初始化加载的数据
     */
    private void load() {
        try {
            channel = FileChannel.open(Paths.get(path), StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);
        } catch (IOException e) {
            LOGGER.error("can not create file : " + path);
            throw new RuntimeException(e.getMessage());
        }
    }
}
