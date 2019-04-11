package com.carl.hadoop.config;

import javassist.CannotCompileException;

/**
 * <p>Title: cn.carl.hadoop AbstractClassMaker</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 20:28
 * @Version 1.0
 */
public abstract class AbstractClassMaker implements ClassMaker {

    @Override
    public Class<?> getWrapperClass() {
        try {
            return this.getCtClass().toClass();
        } catch (CannotCompileException e) {
            throw new RuntimeException("can not compile current CtClass " + getCtClass());
        }
    }
}
