package cn.carl.hadoop.utils;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;

/**
 * 写出数据的线程不安全类,最好是一个线程一个实例,通过ThreadLocal来处理
 * <p>Title: cn.carl.io.nio BufferWriter</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/30 13:15
 * @Version 1.0
 */
public class BufferWriter {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(BufferWriter.class);

    /**
     * 创建一个缓冲区对象
     */
    private final ByteBuffer buffer = ByteBuffer.allocate(1024);

    /**
     * 通过缓冲区循环将数据向通道中写出,
     * 目前作为线程不安全类,但是并发的效率非常低
     *
     * @param str     写出的字符串数据
     * @param channel 写出的通道的对对象
     */
    public void writeChannel(String str, WritableByteChannel channel) {

        //设置数据的一个标志位
        int flag = 0;

        //获取原始数据的字节数据
        byte[] data = str.getBytes(StandardCharsets.UTF_8);

        //在写出数据中的一个循环的标志位
        int cur = 0;

        try {
            //循环写出一个BUFFER大小的字节数据
            while (flag < data.length) {
                //填充BUFFER缓冲区
                for (; cur < buffer.remaining(); cur++) {
                    //如果当前已经读取完整个字节数据,跳出循环
                    if (flag == data.length) {
                        break;
                    }
                    buffer.put(data[flag++]);
                }

                //改成读取模式
                buffer.flip();

                //写出缓冲区的数据
                channel.write(buffer);
                //清空数据,下次读取
                buffer.clear();
                cur = 0;
            }
        } catch (IOException e) {
            LOGGER.error("while writing data occurred a exception : " + e.getMessage());
            e.printStackTrace();
        } finally {
            //清空缓冲区的缓存数据
            buffer.clear();
        }
    }
}
