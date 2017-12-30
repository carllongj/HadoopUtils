package cn.carl.hadoop.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 默认的比较器类
 * <p>Title: cn.carl.hadoop.mr DefaultComparator</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/30 11:17
 * @Version 1.0
 */
public class DefaultComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.compareTo(b);
    }
}
