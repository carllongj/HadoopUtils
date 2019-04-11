package cn.carl.hadoop.config;

import javassist.CtClass;

/**
 * <p>Title: cn.carl.hadoop</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/26 22:15
 * @Version 1.0
 */
public interface ClassMaker {

    /**
     * 获取指定的CtClass对象
     *
     * @return CtClass对象
     */
    CtClass getCtClass();

    /**
     * 获取当前的包装类对象
     *
     * @return 包装类对象的数据
     */
    Class<?> getWrapperClass();

    /**
     * 获取当前包装类的实例对象
     *
     * @return 当前的修改字节码后的对象
     */
    Object getWrapperInstance();
}
