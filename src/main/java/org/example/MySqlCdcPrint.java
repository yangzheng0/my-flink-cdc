package org.example;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MySqlCdcPrint {
    public static void main(String[] args) throws Exception {
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("106.55.2.254")
                .port(8713)
                .databaseList("hh_edu")//库
                .tableList("hh_edu.litemall_admin")//表
                .username("root")
                .password("E76UBP#Y8*k#Sm46")

                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // enable checkpoint
        env.enableCheckpointing(60000);  //checkpoint需要什么条件?com/ververica/cdc/connectors/mysql/source/enumerator/MySqlSourceEnumerator.snapshotState()

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                // set 4 parallel source tasks
                .setParallelism(1)
                .print("最终数据===>").setParallelism(1); // use parallelism 1 for sink to keep message ordering

        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);//5秒执行一次，模式：精准一次性
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60*1000);
        //2.3 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 2*1000));//两次，两秒执行一次
        //2.4 设置job取消后检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);//保留
        //2.5 设置状态后端-->保存到hdfs
        env.setStateBackend(new FsStateBackend("hdfs://10.66.13.193:9000/ck"));
        //2.6 指定操作hdfs的用户
        System.setProperty("HADOOP_USER_NAME", "gaogc");


        env.execute("MySqlCdcPrint");
    }

}
