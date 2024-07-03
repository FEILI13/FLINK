package org.apache.flink.streaming.examples.reconfiguration;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.reConfig.state.ReconfigurableValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.reconfiguration.datasource.WordCountSource;
import org.apache.flink.util.Collector;

/**
 * @Title: MigrateStateTest
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.reconfiguration.datasource
 * @Date 2024/1/20 15:12
 * @description: 测试细粒度迁移时间
 */
public class MigrateStateTest {

	public static void main(String[] args) throws Exception{
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<String> stringDataStreamSource = env.addSource(new WordCountSource());

		KeyedStream<String, String> stringStringKeyedStream = stringDataStreamSource.keyBy(e -> e);

		SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = stringStringKeyedStream.flatMap(
			new RichFlatMapFunction<String, Tuple2<String, Integer>>() {


				private ValueState<String> state;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					ValueStateDescriptor<String> descriptor = new ReconfigurableValueStateDescriptor<>(
						"migrateTest",
						TypeInformation.of(new TypeHint<String>() {
						})
					);

					state = getRuntimeContext().getState(descriptor);
				}

				@Override
				public void flatMap(
					String value,
					Collector<Tuple2<String, Integer>> out) throws Exception {
					String count = state.value();

					if (count == null) {
						count = "this is a long string, have 72 chars totally, which has 150B state! "
							+ "this is a long string, have 72 chars totally, which has 150B state! "
							+ "this is a long string, have 72 chars totally, which has 150B state! "
							+ "this is a long string, have 72 chars totally, which has 150B state! "
							+ "this is a long string, have 72 chars totally, which has 150B state! "
							+ "this is a long string, have 72 chars totally, which has 150B state! "
							+ "this is a long string, have 72 chars totally, which has 150B state! "
							+ "aaaaaaaaaaa";
					}

					//count += 1;

					Tuple2<String, Integer> res = new Tuple2<>(value, 1);

					state.update(count);
					out.collect(res);
				}
			}).setParallelism(3).setMaxParallelism(6);

		tuple2SingleOutputStreamOperator.print();

		env.execute();
	}

}
