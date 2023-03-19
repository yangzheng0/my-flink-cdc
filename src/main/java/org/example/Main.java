//package org.example;
//
//import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.runtime.state.filesystem.FsStateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.CheckpointConfig;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
//
//public class Main {
//    public static void main(String[] args) throws Exception {
//        System.out.println("Hello world!");
//
//        //TODO 1.基础环境
//        //1.1流处理执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //1.2设置并行度
//        env.setParallelism(1);//设置并行度为1方便测试
//
//        //TODO 2.检查点配置
//        //2.1 开启检查点
//        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);//5秒执行一次，模式：精准一次性
//        //2.2 设置检查点超时时间
//        env.getCheckpointConfig().setCheckpointTimeout(60*1000);
//        //2.3 设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2*1000));//两次，两秒执行一次
//        //2.4 设置job取消后检查点是否保留
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//保留
//        //2.5 设置状态后端-->保存到hdfs
//        env.setStateBackend(new FsStateBackend("hdfs://10.66.13.193:9000/ck"));
//        //2.6 指定操作hdfs的用户
//        System.setProperty("HADOOP_USER_NAME", "gaogc");
//
//        //TODO 3.FlinkCDC
//        //3.1 创建MySQLSource
//        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
//                .hostname("106.55.2.254")
//                .port(8713)
//                .databaseList("hh_edu")//库
//                .tableList("hh_edu.litemall_admin")//表
//                .username("root")
//                .password("E76UBP#Y8*k#Sm46")
//                .startupOptions(StartupOptions.initial())//启动的时候从第一次开始读取
//                .deserializer(new MyDeserializationSchemaFunction ())//这里使用自定义的反序列化器将数据封装成json格式
//                .build();
//
//        DataStreamSource<String> dataStreamSource = env.addSource(sourceFunction);
//
//        //3.2 从源端获取数据
//        DataStreamSource<String> sourceDS = env.addSource(sourceFunction);
//        //打印测试
//        sourceDS.print();
//
//        //执行
//        env.execute();
//
//
//
//    }
//}