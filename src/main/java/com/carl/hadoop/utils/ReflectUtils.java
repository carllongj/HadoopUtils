package com.carl.hadoop.utils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Objects;

/**
 * 类的路径使用.  并且不带文件的后缀    如 com.ab.Test
 * 一个用来获取Class对象以及通过Class对象来获取某个类中的方法和属性对象
 *
 * @author as1
 */
public class ReflectUtils {

    /**
     * 判断第一个参数Class对象是否为第二个参数Class的子类
     *
     * @param subClass   子类的Class对象
     * @param superClass 父类的Class对象
     * @return 一个参数是否为第一个参数的子类
     */
    public static boolean isSubClass(Class<?> subClass, Class<?> superClass) {
        //父类Class对象不能为null
        Objects.requireNonNull(superClass);
        //判断是否为去父类
        return superClass.isAssignableFrom(subClass);
    }

    /**
     * 获取当前Class对象指定名称父类的的泛型类型
     *
     * @param clazz          当前指定的Class对象
     * @param superClassName 指定的父类名称的全限定名称
     * @return 参数化类型数组, 如果没有找到则返回null
     */
    public static Type[] getSuperGenericTypes(Class<?> clazz, String superClassName) {

        //传入的Class对象不能为null
        Objects.requireNonNull(clazz);

        while (null != clazz.getSuperclass() && !superClassName.equals((clazz.getSuperclass()).getName())) {
            clazz = clazz.getSuperclass();
        }

        //如果当前类已经到最顶层还是没有指定的全限定名称的类,返回一个空
        if (null == clazz.getSuperclass()) {
            return null;
        }

        //获取当前参数化类型的指定的类型数组
        return ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments();
    }

    /**
     * 通过将字符串转换为Class对象,并且进行返回
     * 可以为目录/目录/目录,也可以是目录.目录.目录
     *
     * @param className
     * @return Class对象
     */
    @SuppressWarnings("rawtypes")
    public static Class getClassObject(String className) {
        if (className.contains("/")) {
            className = className.replaceAll("/", ".");
        }
        if (className.endsWith(".java")) {
            className = className.substring(0, className.length() - 5);
        }
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("没有找到对应的类");
        }
    }
}