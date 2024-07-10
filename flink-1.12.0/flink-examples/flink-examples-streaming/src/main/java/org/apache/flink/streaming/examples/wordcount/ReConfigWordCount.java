package org.apache.flink.streaming.examples.wordcount;

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Meter;

import com.codahale.metrics.MetricRegistry;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.reConfig.state.ReconfigurableHeapValueState;
import org.apache.flink.runtime.reConfig.state.ReconfigurableValueStateDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.examples.wordcount.util.WordCountData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.io.File;
import java.net.InetAddress;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/**
 * @Title: ReconfigWordCount
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.wordcount
 * @Date 2024/1/12 16:29
 * @description: 测试重配置状态
 */
public class ReConfigWordCount {

	public static void main(String[] args) throws Exception{

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		//DataStream<String> text = null;

		final String hostname = "192.168.225.125";
		final int port = 7777;
		//text = env.socketTextStream(hostname, port, "\n");
		DataStreamSource<String> text = env.addSource(new AdjustableRateSource());
		//text = env.fromElements(WordCountData.WORDS);
		SingleOutputStreamOperator<Tuple3<String, Integer, Long>> tuple2SingleOutputStreamOperator = text.flatMap(
			new FlatMapFunction<String, Tuple3<String, Integer, Long>>() {
				@Override
				public void flatMap(
					String value,
					Collector<Tuple3<String, Integer, Long>> out) throws Exception {
					String[] tmp = value.split(":");

					Tuple3<String, Integer, Long> ans = new Tuple3<>(tmp[0], 1, Long.valueOf(tmp[1]));
					//Tuple3<String, Integer, Long> ans = new Tuple3<>(value, 1, System.currentTimeMillis());
					out.collect(ans);
				}
			}).setParallelism(2).setMaxParallelism(32).name("upStream");

		KeyedStream<Tuple3<String, Integer, Long>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(
			e -> e.f0);

		SingleOutputStreamOperator<String> reconfig = tuple2StringKeyedStream.flatMap(
			new RichFlatMapFunction<Tuple3<String, Integer, Long>, String>() {

				//private ValueState<String> state;
				private ValueState<Integer> state;

//				private transient Meter meter;
//				private transient MetricRegistry registry;
//				private transient CsvReporter reporter;
//				private int taskIndex;
//				private String hostname;

				@Override
				public void open(Configuration parameters) throws Exception {
					super.open(parameters);
					ValueStateDescriptor<Integer> descriptor = new ReconfigurableValueStateDescriptor<>(
						"test",
						TypeInformation.of(new TypeHint<Integer>() {
						})
					);

					state = getRuntimeContext().getState(descriptor);

//					taskIndex = getRuntimeContext().getIndexOfThisSubtask();
//					hostname = InetAddress.getLocalHost().getHostName();
//					registry = new MetricRegistry();
//					reporter = CsvReporter.forRegistry(registry)
//						.formatFor(Locale.US)
//						.convertRatesTo(TimeUnit.SECONDS)
//						.build(new File("/home/ftcl/reConfig/flink-1.12.0/log"));
//					reporter.start(5, TimeUnit.SECONDS);
//					this.meter = new Meter();
//					registry.register("process_throughtput"+taskIndex+"@"+hostname, meter);
				}

				@Override
				public void flatMap(
					Tuple3<String, Integer, Long> value,
					Collector<String> out) throws Exception {
					Integer count = state.value();

					if (count == null) {
						count = 0;
					}

					count += 1;
					state.update(count);
					int numberOfCalculations = 50000; // 定义计算次数
					for (int i = 0; i < numberOfCalculations; i++) {
						Math.exp(Math.random()); // 计算指数
						Math.log(Math.random() + 1); // 计算对数，确保参数大于0
					}
					//String res = "key:" + value.f0 + " state:1" + "$" + value.f2;
//					state.update(KeyGroupTest.generateRandomString(3));
					String res = "key:" + value.f0 + " state:"+state.value()
						+ " taskIndex:" + getRuntimeContext().getIndexOfThisSubtask()
						+ " keyGroup:"+ MathUtils.murmurHash(value.f0.hashCode())%32+
					"$" + value.f2;

//					int numberOfCalculations = 2000; // 定义计算次数
////
//					for (int i = 0; i < numberOfCalculations; i++) {
//						Math.exp(Math.random()); // 计算指数
//						Math.log(Math.random() + 1); // 计算对数，确保参数大于0
//					}
//					meter.mark();
					out.collect(res);
				}
			}).setParallelism(3).setMaxParallelism(32).name("ReConfig");

		DataStreamSink<String> stringDataStreamSink = reconfig.addSink(new SinkFunction<String>() {
			int count = 0;
			@Override
			public void invoke(String value, Context context) throws Exception {
//				if(count%500==0) {
					String[] tmp = value.split("\\$");
					long beforeTime = Long.parseLong(tmp[1]);
					long now = System.currentTimeMillis();
					long cost = now - beforeTime;
					System.out.println(tmp[0] + " cost:" + cost + " currentTime:" + now);
//				}
//				count++;
			}
		});
//		reconfig.print();

//		reconfig.keyBy(e -> e).filter(new FilterFunction<String>() {
//			@Override
//			public boolean filter(String value) throws Exception {
//				return true;
//			}
//		}).print();
		env.execute();
	}

}
