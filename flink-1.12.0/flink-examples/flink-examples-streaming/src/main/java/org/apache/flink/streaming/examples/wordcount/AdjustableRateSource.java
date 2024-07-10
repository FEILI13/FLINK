package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class AdjustableRateSource implements SourceFunction<String> {

	private volatile boolean running = true;
	private AtomicLong targetRate = new AtomicLong(200); // 初始速率，每秒生成记录数

	@Override
	public void run(SourceContext<String> ctx) throws Exception {
		System.out.println("connecting...");
		// 启动一个线程来监听 socket 连接以调整速率
		new Thread(() -> {
			try (ServerSocket serverSocket = new ServerSocket(9999, 50, InetAddress.getByName("0.0.0.0"))) {
				System.out.println("Server started, waiting for connections...");
				while (running) {
					try (Socket clientSocket = serverSocket.accept();
						 BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()))) {
						System.out.println("Client connected: " + clientSocket.getInetAddress());
						String inputLine;
						while ((inputLine = in.readLine()) != null) {
							long newRate = Long.parseLong(inputLine);
							targetRate.set(newRate);
							System.out.println("Rate set to: " + newRate);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();

		Random random = new Random();
		long lastTimestamp = System.currentTimeMillis();
		int count = 0;

		while (running) {
			if (count >= targetRate.get()) {
				long currentTimestamp = System.currentTimeMillis();
				long elapsed = currentTimestamp - lastTimestamp;
				if (elapsed < 1000) {
					// 如果还没到1秒，等待剩余时间
					Thread.sleep(1000 - elapsed);
				}
				// 重置计数和时间
				lastTimestamp = currentTimestamp;
				count = 0;
			}

			int tmp = random.nextInt(10);
			String value = generateValue(tmp);
			synchronized (ctx.getCheckpointLock()) {
				ctx.collect(value);
			}
			count++;
		}
	}

	private String generateValue(int tmp) {
//		if (tmp < 1) {
//			return KeyGroupTest.generate(0, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 2) {
//			return KeyGroupTest.generate(2, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 3) {
//			return KeyGroupTest.generate(3, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 4) {
//			return KeyGroupTest.generate(4, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 5) {
//			return KeyGroupTest.generate(5, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 6) {
//			return KeyGroupTest.generate(7, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 7) {
//			return KeyGroupTest.generate(8, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 8) {
//			return KeyGroupTest.generate(9, 3) + ":" + System.currentTimeMillis();
//		} else if (tmp < 9) {
//			return KeyGroupTest.generate(10, 3) + ":" + System.currentTimeMillis();
//		} else {
			return KeyGroupTest.generateRandomString(15) + ":" + System.currentTimeMillis();
//		}
	}

	@Override
	public void cancel() {
		running = false;
	}

	public void setRate(long newRate) {
		targetRate.set(newRate);
	}
}
