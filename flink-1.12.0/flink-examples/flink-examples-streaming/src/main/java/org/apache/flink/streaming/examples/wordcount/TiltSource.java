package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Title: TiltSource
 * @Author lxisnotlcn
 * @Package org.apache.flink.streaming.examples.wordcount
 * @Date 2024/3/30 9:20
 * @description: 倾斜数据源
 */
public class TiltSource implements SourceFunction<String> {

	private boolean running = true;
	int total = 0;

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		while (running){
			Random random = new Random();
			int tmp = random.nextInt(10);
			if(tmp<1){
				total++;
				ctx.collect(KeyGroupTest.generate(0,3)+":"+System.currentTimeMillis());
			} else if (tmp<2) {
				total++;
				ctx.collect(KeyGroupTest.generate(2,3)+":"+System.currentTimeMillis());
			} else if (tmp<3) {
				total++;
				ctx.collect(KeyGroupTest.generate(3,3)+":"+System.currentTimeMillis());
			} else if (tmp<4) {
				total++;
				ctx.collect(KeyGroupTest.generate(4,3)+":"+System.currentTimeMillis());
			} else if (tmp<5) {
				total++;
				ctx.collect(KeyGroupTest.generate(5,3)+":"+System.currentTimeMillis());
			} else if (tmp<6) {
				total++;
				ctx.collect(KeyGroupTest.generate(7,3)+":"+System.currentTimeMillis());
			} else if (tmp<7) {
				total++;
				ctx.collect(KeyGroupTest.generate(8,3)+":"+System.currentTimeMillis());
			} else if (tmp<8) {
				total++;
				ctx.collect(KeyGroupTest.generate(9,3)+":"+System.currentTimeMillis());
			} else if(tmp<9){
				total++;
				ctx.collect(KeyGroupTest.generate(10,3)+":"+System.currentTimeMillis());
			}else{
				ctx.collect(KeyGroupTest.generateRandomString(3)+":"+System.currentTimeMillis());
			}
			TimeUnit.MILLISECONDS.sleep(1);
			//Thread.sleep(1);
//			if(total % 10000==0){
//				Thread.sleep(80);
//			}
		}
	}

	@Override
	public void cancel() {
		running = false;
	}

}
