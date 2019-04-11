package cn.carl.hadoop.run;

/**
 * 保存一些启动任务的常量参数
 * <p>Title: cn.carl.hadoop.run RunnerParam</p>
 * <p>Description: </p>
 * <p>Company: </p>
 *
 * @author carl
 * @date 2017/12/27 19:48
 * @Version 1.0
 */
public class RunnerParam {

    /**
     * 设置当前是否执行setup方法
     */
    protected boolean executeSetup;

    /**
     * 设置当前是否执行cleanup方法
     */
    protected boolean executeCleanup;

    /**
     * 设置当前调用初始化方法的名称
     */
    protected String initMethod;

    /**
     * 设置当前调用回调方法的名称
     */
    protected String callback;

    public boolean isExecuteSetup() {
        return executeSetup;
    }

    public void setExecuteSetup(boolean executeSetup) {
        this.executeSetup = executeSetup;
    }

    public boolean isExecuteCleanup() {
        return executeCleanup;
    }

    public void setExecuteCleanup(boolean executeCleanup) {
        this.executeCleanup = executeCleanup;
    }

    public String getInitMethod() {
        return initMethod;
    }

    public void setInitMethod(String initMethod) {
        this.initMethod = initMethod;
    }

    public String getCallback() {
        return callback;
    }

    public void setCallback(String callback) {
        this.callback = callback;
    }

    /**
     * 默认构造器,没有初始化方法,回调,也不设置当前的setup和
     * cleanup方法
     */
    public RunnerParam() {
    }

    public RunnerParam(String initMethod, String callback) {
        this.initMethod = initMethod;
        this.callback = callback;
    }

    public RunnerParam(boolean executeSetup, boolean executeCleanup) {
        this.executeSetup = executeSetup;
        this.executeCleanup = executeCleanup;
    }

    public RunnerParam(boolean executeSetup, boolean executeCleanup,
                       String initMethod, String callback) {
        this.executeSetup = executeSetup;
        this.executeCleanup = executeCleanup;
        this.initMethod = initMethod;
        this.callback = callback;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RunnerParam that = (RunnerParam) o;

        if (executeSetup != that.executeSetup) {
            return false;
        }
        if (executeCleanup != that.executeCleanup) {
            return false;
        }
        if (initMethod != null ? !initMethod.equals(that.initMethod) : that.initMethod != null) {
            return false;
        }
        return callback != null ? callback.equals(that.callback) : that.callback == null;

    }

    @Override
    public int hashCode() {
        int result = (executeSetup ? 1 : 0);
        result = 31 * result + (executeCleanup ? 1 : 0);
        result = 31 * result + (initMethod != null ? initMethod.hashCode() : 0);
        result = 31 * result + (callback != null ? callback.hashCode() : 0);
        return result;
    }
}
