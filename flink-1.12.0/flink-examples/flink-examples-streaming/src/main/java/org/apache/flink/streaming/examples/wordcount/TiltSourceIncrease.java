package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Title: TiltSourceIncrease
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.wordcount
 * @Date 2024/4/6 13:19
 * @description:
 */
public class TiltSourceIncrease implements SourceFunction<String> {

	private boolean running = true;
	int total = 0;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running){
			Random random = new Random();
			int tmp = random.nextInt(10);
			if(tmp<7){
				total++;
				ctx.collect(KeyGroupTest.generate(2,450)+":"+System.currentTimeMillis());
//			} else if (tmp<4) {
//				total++;
//				ctx.collect(KeyGroupTest.generate(3,3)+":"+System.currentTimeMillis());
//			} else if (tmp<6) {
//				total++;
//				ctx.collect(KeyGroupTest.generate(5,3)+":"+System.currentTimeMillis());
//			} else if (tmp<8) {
//				total++;
//				ctx.collect(KeyGroupTest.generate(10,7)+":"+System.currentTimeMillis());
			} else {
				ctx.collect(KeyGroupTest.generateRandomString(3)+":"+System.currentTimeMillis());
			}
			//TimeUnit.MICROSECONDS.sleep(30);
			Thread.sleep(10);
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
