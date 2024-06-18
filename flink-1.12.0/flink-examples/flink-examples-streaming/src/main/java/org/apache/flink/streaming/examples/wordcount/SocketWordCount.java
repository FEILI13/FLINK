package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Title: SocketWordCount
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.wordcount
 * @Date 2024/1/7 15:15
 * @description: socket word count
 */
public class SocketWordCount {
	public static void main(String[] args) throws Exception{

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> text = null;
		System.out.println("Executing WordCount example with default input data set.");
		System.out.println("Use --input to specify file input.");
		// get default test text data
		//text = env.fromElements(WordCountData.WORDS);

		final String hostname = "192.168.225.125";
		final int port = 12345;
		text = env.socketTextStream(hostname, port, "\n");

		SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = text.flatMap(
			new FlatMapFunction<String, Tuple2<String, Integer>>() {
				@Override
				public void flatMap(
					String value,
					Collector<Tuple2<String, Integer>> out) throws Exception {
					Tuple2<String, Integer> tmp = new Tuple2<>(value, 1);
					out.collect(tmp);
				}
			});

		KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(
			e -> e.f0);

		SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = tuple2StringKeyedStream.flatMap(
			new RichFlatMapFunction<Tuple2<String, Integer>, String>() {

				private ValueState<Integer> state1;
				private ValueState<Integer> state2;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<>(
						"wordcount_state_1",
						TypeInformation.of(
							new TypeHint<Integer>() {
							}));
					ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<>(
						"wordcount_state_2",
						TypeInformation.of(
							new TypeHint<Integer>() {
							}));
					state1 = getRuntimeContext().getState(descriptor1);
					state2 = getRuntimeContext().getState(descriptor2);
				}

				@Override
				public void flatMap(
					Tuple2<String, Integer> value,
					Collector<String> out) throws Exception {
					Integer count1 = state1.value();
					Integer count2 = state2.value();

					if (count1 == null) {
						count1 = 0;
					}
					if (count2 == null) {
						count2 = 0;
					}

					count1 += 1;
					count2 += 2;

					String res = "key:" + value.f0 + " state1:" + count1 + " state2:" + count2;

					state1.update(count1);
					state2.update(count2);
					out.collect(res);
				}
			});

		stringSingleOutputStreamOperator.print();
		env.execute();
	}
}
