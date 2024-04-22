package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class StreamTest {
	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.enableCheckpointing(1000, CheckpointingMode.AT_LEAST_ONCE);
//        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //env.enableCheckpointing(50000);
		DataStreamSource<String> lineDSS = env.socketTextStream("192.168.225.51",
			7777);
//        DataStreamSource<String> lineDSS = env.fromElements(
//                "Flink", "Apache", "Streaming", "BigData", "WordCount", "RandomData", "FlinkStreaming"
//        );
		SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
			.flatMap((String line, Collector<String> words) -> {
				Arrays.stream(line.split(" ")).forEach(words::collect);
			})
			.returns(Types.STRING)
			.map(word -> Tuple2.of(word, 1L))
			.returns(Types.TUPLE(Types.STRING, Types.LONG))
			.setParallelism(2);

		KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
			.keyBy(t -> t.f0);

		SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
			.sum(1);

		Thread.sleep(10000);

		result.print();

		env.execute();


	}



}
