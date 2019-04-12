## 一个简单的Hadoop的MapReduce测试程序

### 使用方式
```
    //指定当前运行的输入文件数据路径列表
    String[] inputPaths = new String[]{"/data/input/a","/data/input/b"};

    //指定当前的输出数据路径
    String outputPath = "/data/output";

    //开始执行mr测试逻辑
    HadoopUtils.runMapReduce(inputPaths,outputPath,test.Mapper.class);
```

### 指定输出方式
```
    通过WriteStrategy接口来将对应的数据输出方式
        1.ConsoleWrite   控制台输出
        2.FileWrite      本地文件输出
```

### 优势
 + 代码零侵入,对源代码没有侵入
 + 代码简单调试,不需要在集群上进行调试

### 实现
  通过生成指定的Class<? extends Mapper> 或者 Class<? extend Reducer>的Class的子类,
  在子类中调用父类的map方法或者reduce方法,达到执行MapReduce中的逻辑
  