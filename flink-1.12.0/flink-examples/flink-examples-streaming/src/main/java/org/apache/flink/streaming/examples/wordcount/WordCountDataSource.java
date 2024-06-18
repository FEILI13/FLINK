package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Title: WordCountDataSource
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.wordcount
 * @Date 2024/3/14 19:47
 * @description: 测试用datasource
 */
public class WordCountDataSource implements SourceFunction<String> {

	private boolean running = true;
	int total = 0;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running){
			Random random = new Random();
			if(random.nextInt(10)<9){
				total++;
				ctx.collect(KeyGroupTest.generate17(50)+":"+System.currentTimeMillis());
			}else {
				ctx.collect(KeyGroupTest.generateRandomString(50)+":"+System.currentTimeMillis());
			}
			TimeUnit.MICROSECONDS.sleep(30);
			if(total > 300000){
				running = false;
			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}
}
