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