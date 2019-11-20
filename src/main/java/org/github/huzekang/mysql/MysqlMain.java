package org.github.huzekang.mysql;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: flink-starter
 * @author: huzekang
 * @create: 2019-11-20 16:01
 **/
@Slf4j
public class MysqlMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Map> mapDataStreamSource = env.addSource(new SourceFromMysql());

        mapDataStreamSource.addSink(new SinkToMySQL());

        env.execute("flink mysql source and sink run");
    }
}
