package cn.carl.hadoop.utils;

import javassist.*;
import org.apache.log4j.Logger;

import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 提供简单的对javassist的一些操作
 * <p>Title: AssistUtils</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2016年08月1日 下午9:35:54
 * @Version 1.0
 */
public class AssistUtils {

    /**
     * 日志对象
     */
    private static final Logger LOGGER = Logger.getLogger(AssistUtils.class);

    /**
     * 定义使用单例模式获取对象池的对象
     */
    private static final ReadWriteLock rwl = new ReentrantReadWriteLock();

    /**
     * 当前的类池对象
     */
    private static ClassPool loaderClassPool = null;

    /**
     * 简化对获取的操作,如果只是单单获取默认的,会出现找不到类的异常
     * 使用静态内部类直接创建单例对象
     *
     * @return
     */
    public static ClassPool getClassPool() {
        return InnerClassPool.classPool;
    }

    /**
     * 静态内部类创建单例模式
     * <p>Title: InnerClassPool</p>
     * <p>Description: </p>
     * <p>Company: </p>
     *
     * @author carl
     * @date 2016年12月1日 上午10:19:52
     * @Version 1.0
     */
    private static class InnerClassPool {
        private static final ClassPool classPool;

        static {
            LOGGER.info("current javassist version is " + CtClass.version);
            classPool = ClassPool.getDefault();
        }
    }

    /**
     * 使用读写锁来控制单例模式的共享
     *
     * @param loader 传入需要使用的加载器的ClassLoader对象来获取类池,
     *               如果传入为null,那么将使用当前线程上下文加载器对象
     * @return javaAssist类池对象
     */
    public static ClassPool getClassPool(ClassLoader loader) {

        LOGGER.info("current javassist version is " + CtClass.version);

        //当前类加载器为null,则将类加载器置为线程上下文类加载器
        if (null == loader) {
            loader = Thread.currentThread().getContextClassLoader();
        }
        try {
            rwl.readLock().lock();
            if (loaderClassPool == null) {
                try {
                    //释放读锁
                    rwl.readLock().unlock();
                    //加上写锁
                    rwl.writeLock().lock();
                    if (loaderClassPool == null) {
                        ClassPool cp = new ClassPool(true);
                        cp.appendClassPath(new LoaderClassPath(loader));
                        loaderClassPool = cp;
                        return cp;
                    }
                } finally {
                    rwl.readLock().lock();
                    rwl.writeLock().unlock();
                }
            }
        } finally {
            rwl.readLock().unlock();
        }
        return null;
    }

    /**
     * 通过类的全限定名称来获取该类的CtClass对象
     *
     * @param className 类的全限定名称
     * @return 当前指定类的CtClass对象
     */
    public static CtClass getCtClassByName(String className) {
        final ClassPool classPool = InnerClassPool.classPool;
        try {

            //获取当前指定类的CtClass对象
            return classPool.get(className);

        } catch (NotFoundException e) {
            LOGGER.error("class " + className + "Not Found.");
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 给指定的CtClass对象添加一个构造器
     *
     * @param body    执行的方法体字符串
     * @param params  参数的CtClass数组类型
     * @param ctClass 添加到指定的CtClass对象上
     */
    public static void addConstructor(String body, CtClass[] params, CtClass ctClass) {

        //创建一个构造器
        CtConstructor ctConstructor = new CtConstructor(params, ctClass);

        try {
            //设置当前构造器的方法内容
            ctConstructor.setBody(body);

            //添加对应的构造器对象
            ctClass.addConstructor(ctConstructor);
        } catch (CannotCompileException e) {
            LOGGER.error("can not compile constructor " + body + " " + e.getMessage());
            throw new RuntimeException(e.getMessage());
        }
    }

    /**
     * 获取CtClass的一个指定方法名称的CtMethod对象
     *
     * @param ctClass    获取的CtClass对象
     * @param methodName 方法名称
     * @param param      参数列表
     * @return 获取的CtMethod对象
     */
    public static CtMethod getMethod(CtClass ctClass, String methodName, CtClass[] param) {
        Objects.requireNonNull(ctClass);

        try {
            return ctClass.getDeclaredMethod(methodName, param);
        } catch (NotFoundException e) {
            LOGGER.error("Method " + methodName + "Not Found");
            throw new RuntimeException(e.getMessage());
        }
    }
}