package org.apache.flink.streaming.examples.reconfiguration.datasource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @Title: WordCountSource
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.reconfiguration.datasource
 * @Date 2024/1/20 15:03
 * @description: wordcount数据源
 */
public class WordCountSource implements SourceFunction<String> {

	private boolean running = true;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while(running) {
			//String data = String.valueOf(System.currentTimeMillis());
			ctx.collect(String.valueOf(System.currentTimeMillis()));
			Thread.sleep(1);
		}

	}

	@Override
	public void cancel() {
		running = false;
	}
}
