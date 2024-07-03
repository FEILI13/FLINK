package org.apache.flink.runtime.reConfig;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.reConfig.partition.LoadBanlancer;
import org.apache.flink.runtime.reConfig.partition.MigratePlanEntry;
import org.apache.flink.runtime.reConfig.partition.PartitionManager;
import org.apache.flink.runtime.reConfig.rescale.RescaleManager;
import org.apache.flink.runtime.reConfig.utils.HttpMetricClient;
import org.apache.flink.runtime.reConfig.utils.InstanceState;
import org.apache.flink.runtime.reConfig.message.ReConfigSignal;
import org.apache.flink.runtime.reConfig.message.ReConfigStage;
import org.apache.flink.runtime.reConfig.utils.ReConfigResultCollector;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Title: Controller
 * @Author lxisnotlcn
 * @Package org.apache.flink.runtime.reConfig
 * @Date 2024/2/28 20:58
 * @description: 全局控制器
 */
public class Controller {

	private final JobID jobID;
	private ExecutionVertex[] tasksToTrigger;
	private ExecutionVertex[] tasksToWaitFor;
	private ExecutionVertex[] tasksToCommitTo;
	private ExecutionVertex[] upStreamExecutionVertex;//上游需要改变路由的实例
	private ExecutionVertex[] reConfigExecutionVertex;//需要重配置的实例
	private Set<ExecutionAttemptID> upStreamAttemptSet;
	private Set<ExecutionAttemptID> reConfigAttemptSet;
	private int upStreamToWait;
	private int reConfigToWait;
	private int maxParallelism;// reConfig算子的最大并行度（计算键组）
	private Map<Integer, Integer> keyGroupTaskMap;//记录键组与算子实例之间的映射关系
	private ReConfigResultCollector reConfigResultCollector = new ReConfigResultCollector();
	private Thread periodDetectThread;//周期性获取状态的线程

	private PartitionManager partitionManager;
	private RescaleManager rescaleManager;
	private AtomicLong version = new AtomicLong(0);
	private int test = 0;
	private int threshold;

	private ScheduledFuture<?> currentTrigger;
	private Map<Integer, Integer> taskRecordCountMap;

	private int currentbatch;
	private int currentSourceIndex;
	private int currentTargetIndex;
	private int currentSplitNum;
	private boolean haveTrigger = false;

	//private static final String CONF_PATH = "/home/ftcl/reConfig/config.properties";
	private static final String CONF_PATH = "C:\\Users\\17728\\Desktop\\config.properties";
	private int initDelay;//初始间隔时间
	private int delay;//间隔时间
	private boolean enable;
	private String jobManagerIP;
	private int minWidth;
	private Map<Integer, Map<Integer, Integer>> stateInfo;
	private LoadBanlancer loadBanlancer;

	private boolean triggerRescale = false;
	//private MigratePlanEntry bigEntry = null;//  超过200Mb的大状态条目

	public Controller(
		JobID jobID,
		ExecutionVertex[] tasksToTrigger,
		ExecutionVertex[] tasksToWaitFor,
		ExecutionVertex[] tasksToCommitTo,
		ExecutionVertex[] upStreamExecutionVertex,
		ExecutionVertex[] reConfigExecutionVertex) {
		this.tasksToTrigger = tasksToTrigger;
		this.tasksToWaitFor = tasksToWaitFor;
		this.tasksToCommitTo = tasksToCommitTo;
		this.jobID = jobID;
		this.upStreamExecutionVertex = upStreamExecutionVertex;
		this.reConfigExecutionVertex = reConfigExecutionVertex;
		this.upStreamToWait = upStreamExecutionVertex.length;
		this.reConfigToWait = reConfigExecutionVertex.length;
		this.keyGroupTaskMap = new HashMap<>();
		this.partitionManager = new PartitionManager(tasksToTrigger, tasksToWaitFor, tasksToCommitTo, upStreamExecutionVertex);
		this.rescaleManager = new RescaleManager(partitionManager, tasksToTrigger, tasksToWaitFor, tasksToCommitTo, upStreamExecutionVertex);
		this.loadBanlancer = new LoadBanlancer();
	}

	private Execution[] getTriggerExecutions() throws CheckpointException {
		Execution[] executions = new Execution[tasksToTrigger.length];
		for(int i=0;i<tasksToTrigger.length;++i){
			Execution ee = tasksToTrigger[i].getCurrentExecutionAttempt();
			if(ee == null){
				throw new CheckpointException(CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}else if(ee.getState() == ExecutionState.RUNNING){
				executions[i] = ee;
			}else{
				throw new CheckpointException(
					CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
			}
		}
		return executions;
	}


	public void handle(ReConfigStage stage) {
//		if(stage == ReConfigStage.INIT) {
//			test++;
//			if (test == 4) {
//				taskRecordCountMap = new ConcurrentHashMap<>();
//				ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
//					new DispatcherThreadFactory(
//						Thread.currentThread().getThreadGroup(), "controller-query-state"
//					));
//				ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(
//					scheduledExecutorService);
//				currentTrigger = scheduledExecutor.scheduleAtFixedRate(
//					new QueryState(),
//					1000,
//					10000,
//					TimeUnit.MILLISECONDS);
//			}
//		} else if (stage == ReConfigStage.MIGRATED) {
//			if(currentbatch<currentSplitNum) {
//				try {
//					Thread.sleep(10000);
//				}catch (Exception e){
//					e.printStackTrace();
//				}
//				partitionManager.migrte(
//					currentSourceIndex,
//					currentTargetIndex,
//					17,
//					currentbatch++,
//					currentSplitNum,
//					version.addAndGet(1));
//			}
//		}
	}

	public synchronized void handle(InstanceState state){
//		taskRecordCountMap.put(state.getTaskIndex(), state.getRecordCount());
//		if(!haveTrigger&&state.getRecordCount()>20) {// BCE 17
//			haveTrigger = true;
//			currentTrigger.cancel(true);
//			currentbatch = 0;
//			currentSourceIndex = 0;
//			currentTargetIndex = 1;
//			currentSplitNum = 2;
//			partitionManager.migrte(currentSourceIndex, currentTargetIndex, 17, currentbatch++, currentSplitNum, version.addAndGet(1));
//		}
	}

	public void handle(ReConfigSignal signal){
		if(signal.getType() == ReConfigSignal.ReConfigSignalType.INIT){
			//System.out.println("get init message from"+signal.getProperties("taskName"));
			if(((String)signal.getProperties("taskName")).contains("upStream")){
				upStreamToWait--;
			}else if(((String)signal.getProperties("taskName")).contains("ReConfig")){
				reConfigToWait--;
			}
			maxParallelism = (int) signal.getProperties("maxParallelism");
			if(upStreamToWait == 0 && reConfigToWait == 0){

				Properties prop = new Properties();
				try {
					// 加载.properties文件
					prop.load(Files.newInputStream(Paths.get(CONF_PATH)));
					// 获取属性值
					threshold = Integer.valueOf(prop.getProperty("threshold"));
					currentSplitNum = Integer.valueOf(prop.getProperty("splitNum"));
					initDelay = Integer.valueOf(prop.getProperty("initDelay"));
					delay = Integer.valueOf(prop.getProperty("delay"));
					enable = Boolean.valueOf(prop.getProperty("enable"));
					//jobManagerIP = prop.getProperty("jobManagerIP");
					//minWidth = Integer.valueOf(prop.getProperty("minWidth"));
				} catch (IOException ex) {
					ex.printStackTrace();
				}

				upStreamAttemptSet = new HashSet<>();
				reConfigAttemptSet = new HashSet<>();
				stateInfo = new HashMap<>();
				for (int i = 0; i < reConfigExecutionVertex.length; ++i) {// 初始化状态信息
					stateInfo.put(i, new HashMap<>());
				}
				for (ExecutionVertex e : upStreamExecutionVertex) {
					upStreamAttemptSet.add(e.getCurrentExecutionAttempt().getAttemptId());
				}
				for (ExecutionVertex e : reConfigExecutionVertex) {
					reConfigAttemptSet.add(e.getCurrentExecutionAttempt().getAttemptId());
				}
				int parallelism = reConfigExecutionVertex.length;
				for(int i=0;i<maxParallelism;++i){//初始化每个键组所处的实例位置
					this.keyGroupTaskMap.put(i, i * parallelism / maxParallelism);
				}
				System.out.println("now map:"+keyGroupTaskMap);

				if(enable) {
					ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(
						new DispatcherThreadFactory(
							Thread.currentThread().getThreadGroup(), "controller-query-state"
						));
					ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(
						scheduledExecutorService);
//					currentTrigger = scheduledExecutor.scheduleWithFixedDelay(
//						new QueryState(),
//						initDelay,
//						delay,
//						TimeUnit.SECONDS);
					currentTrigger = scheduledExecutor.schedule(new QueryState(), initDelay, TimeUnit.SECONDS);
				}else{
					new Thread(()->{
						try {
							int port = 12345;
							ServerSocket serverSocket = new ServerSocket(port);
							//System.out.println("Server is listening on port " + port);

							while (true) {
								Socket socket = serverSocket.accept();

								BufferedReader input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
								StringBuilder messageBuilder = new StringBuilder();
								String line;
								while ((line = input.readLine()) != null && !line.isEmpty()) {
									messageBuilder.append(line);
								}
								JSONObject json = JSON.parseObject(messageBuilder.toString()); // 使用FastJson解析JSON字符串

								if(json.containsKey("increase") || json.containsKey("migrate")){
									int keyGroupIndex = Integer.parseInt((String) json.get("keygroup"));
									long tmp = version.addAndGet(1);
									CompletableFuture<Acknowledge> acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
										reConfigAttemptSet,
										tmp);
									final Execution[] executions = getTriggerExecutions();
									for(Execution execution:executions){
										execution.triggerReConfig(
											new ReConfigSignal(
												ReConfigSignal.ReConfigSignalType.INCREASE_STATE,
												tmp
											)
										);
									}
									acknowledgeCompletableFuture.get();

									MigratePlanEntry entry = loadBanlancer.increaseSave(
										keyGroupIndex,
										reConfigExecutionVertex.length);

									if(json.containsKey("network")){
										while(true) {
											StringBuilder sb = new StringBuilder();
											HttpMetricClient httpMetricClient = new HttpMetricClient(
												jobManagerIP);
											Map metric = httpMetricClient.GetNetworkMetric();
											AtomicReference<Double> v = new AtomicReference<>(0.0);
											metric.forEach((ip1, value1) -> {
												Map ipTable = (Map) value1;
												ipTable.forEach((ip2, value2) -> {
													if(ip1.equals("192.168.225.125")){
														Map metricTable = (Map) value2;
														try {
															v.set(Math.max(Double.parseDouble(((String) (metricTable.get(
																"wdith"))).split(" ")[0]), v.get()));
														}catch (Exception e){
															e.printStackTrace();
															v.set(0.0);
														}
													}
												});
											});
											System.out.println("max bandwidth:"+v.get());
											if(v.get()*1000_000 < minWidth){
												Thread.sleep(100);
											}else{
												break;
											}
										}
									}

									if(json.containsKey("splitNum")){
										entry.setSplitNum(Integer.parseInt((String) json.get("splitNum")));
									}

									//entry.setSplitNum(currentSplitNum);

									if(json.containsKey("migrate")){
										entry.setTargetTask(Integer.parseInt((String) json.get("target")));
									}

									if(entry.getSourceTask() == entry.getTargetTask()){
										socket.close();
										continue;
									}

									for(int i=0;i<entry.getSplitNum();++i) {
										partitionManager.migrte(entry, i, version.addAndGet(1));
									}
									loadBanlancer.clear();
									cleanStage();

								}else if(json.containsKey("reConfig")){
									QueryState q = new QueryState();
									q.run();
								} else if(json.containsKey("network")){
									HttpMetricClient httpMetricClient = new HttpMetricClient(
										jobManagerIP);
									Map metric = httpMetricClient.GetNetworkMetric();
									metric.forEach((ip1, value1) -> {
										Map ipTable = (Map) value1;
										ipTable.forEach((ip2, value2) -> {
											Map metricTable = (Map) value2;
											System.out.println(ip1 + " -> " + ip2 + " " + metricTable.toString());
										});
									});
								} else if(json.containsKey("net")){
									HttpMetricClient httpMetricClient = new HttpMetricClient(
										jobManagerIP);
									while(true) {
										Map metric = httpMetricClient.GetNetworkMetric();
										System.out.println("++++++++++++++++++++++++++");
										metric.forEach((ip1, value1) -> {
											Map ipTable = (Map) value1;
											ipTable.forEach((ip2, value2) -> {
												Map metricTable = (Map) value2;
												System.out.println(ip1 + " -> " + ip2 + " "
													+ metricTable.toString());
											});
										});
										Thread.sleep(100);
									}
								}

								socket.close(); // 关闭连接
							}
						}catch (Exception e){
							e.printStackTrace();
							throw new UnsupportedOperationException("参数异常");
						}
					}).start();
				}
//				periodDetectThread = new Thread(new QueryState());
//				periodDetectThread.start();
			}
		}else if(signal.getType() == ReConfigSignal.ReConfigSignalType.PERIOD_RESULT){
//			stateInfo.get((Integer) signal.getProperties("taskIndex")).put(
//				((int[]) signal.getProperties("keyGroupStateSize"))[0],
//				((int[]) signal.getProperties("keyGroupStateSize"))[1]
//			);
//			if(loadBanlancer.migratePlan.isFinish()) {
				//loadBanlancer.clear();
			loadBanlancer.putFrequencyWindowInfo((Map<Integer, Integer>) (signal.getProperties(
				"frequencyWindowInfo")));
			loadBanlancer.putSizeWindowInfo((Map<Integer, Long>) (signal.getProperties(
				"sizeWindowInfo")));
			System.out.println("sizeWindowInfo:"+((Map<Integer, Long>) (signal.getProperties(
				"sizeWindowInfo"))));
			for (Map.Entry<Integer, Integer> entry : ((Map<Integer, Integer>) (signal.getProperties(
				"frequencyWindowInfo"))).entrySet()) {
				System.out.println("keyGroup:" + entry.getKey() + " frequency:" + entry.getValue());
			}
			Map<Integer, Integer> keyGroupMap = new HashMap<>();
			for (Integer keyGroupIndex : ((Map<Integer, Integer>) (signal.getProperties(
				"frequencyWindowInfo"))).keySet()) {
//					System.out.println(
//						"get message:" + keyGroupIndex + " from " + (Integer) signal.getProperties(
//							"taskIndex"));
				keyGroupMap.put(keyGroupIndex, (Integer) signal.getProperties("taskIndex"));
			}
			loadBanlancer.putKeyGroupMap(keyGroupMap);
//			}
			reConfigResultCollector.acknowledgeRescale(
				(ExecutionAttemptID) signal.getProperties("executionId"),
				signal.getVersion()
			);
		}else if(signal.getType() == ReConfigSignal.ReConfigSignalType.MIGRATE_COMPLETE){
			//System.out.println("get ID:"+(ExecutionAttemptID) signal.getProperties("executionId"));
			reConfigResultCollector.acknowledgeRescale(
				(ExecutionAttemptID) signal.getProperties("executionId"),
				signal.getVersion()
			);
		} else if (signal.getType() == ReConfigSignal.ReConfigSignalType.CLEAN_COMPLETE) {
			reConfigResultCollector.acknowledgeRescale(
				(ExecutionAttemptID) signal.getProperties("executionId"),
				signal.getVersion()
			);
		} else if (signal.getType() == ReConfigSignal.ReConfigSignalType.RESCALE_COMPLETE){
			rescaleManager.ackRescale(signal);
		}else if(signal.getType() == ReConfigSignal.ReConfigSignalType.INCREASE_RESULT){// 收集键组状态大小信息
			loadBanlancer.putSizeWindowInfo((Map<Integer, Long>) (signal.getProperties(
				"sizeWindowInfo")));
			Map<Integer, Integer> keyGroupMap = new HashMap<>();
			for (Integer keyGroupIndex : ((Map<Integer, Integer>) (signal.getProperties(
				"sizeWindowInfo"))).keySet()) {
//				System.out.println("keyGroupIndex:"+keyGroupIndex+" stateSize:"+((Map<Integer, Integer>) (signal.getProperties(
//					"sizeWindowInfo"))).get(keyGroupIndex));
				keyGroupMap.put(keyGroupIndex, (Integer) signal.getProperties("taskIndex"));
			}
			loadBanlancer.putKeyGroupMap(keyGroupMap);
			reConfigResultCollector.acknowledgeRescale(
				(ExecutionAttemptID) signal.getProperties("executionId"),
				signal.getVersion()
			);
		}
	}

	/**
	 * 根据负载均衡程度决定是否迁移
	 */
	public void mayBeReBalance(){
//		for(Integer i:stateInfo.keySet()){
//			//System.out.println("task "+i+" info:");
//			for(Map.Entry<Integer, Integer> entry:stateInfo.get(i).entrySet()){
//				if(entry.getValue()>=threshold && !haveTrigger){
//					Set<ExecutionAttemptID> tmp = new HashSet<>();
//					tmp.add(reConfigExecutionVertex[i].getCurrentExecutionAttempt().getAttemptId());
//					tmp.add(reConfigExecutionVertex[(i+1)%reConfigExecutionVertex.length].getCurrentExecutionAttempt()
//						.getAttemptId());
//					//System.out.println("source ID:" + reConfigExecutionVertex[i].getCurrentExecutionAttempt().getAttemptId());
//					//System.out.println("target ID:" + reConfigExecutionVertex[(i+1)%reConfigExecutionVertex.length].getCurrentExecutionAttempt()
//					//	.getAttemptId());
//					try {
//						CompletableFuture<Acknowledge> acknowledgeCompletableFuture;
//						for (int batch = 0; batch < currentSplitNum; ++batch) {
//							acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
//								tmp,
//								version.addAndGet(1));
//							partitionManager.migrte(
//								i,
//								(i + 1) % reConfigExecutionVertex.length,
//								entry.getKey(),
//								batch,
//								currentSplitNum,
//								version.get());
//							acknowledgeCompletableFuture.get();
//							//Thread.sleep(5000);
//						}
//						haveTrigger = true;
//
//						// clean阶段
//						tmp.clear();
//						for(ExecutionVertex upstream:upStreamExecutionVertex){
//							tmp.add(upstream.getCurrentExecutionAttempt().getAttemptId());
//						}
//						for(ExecutionVertex reConfig:reConfigExecutionVertex){
//							tmp.add(reConfig.getCurrentExecutionAttempt().getAttemptId());
//						}
//						acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
//							tmp,
//							version.addAndGet(1));
//						partitionManager.clean(version.get());
//						acknowledgeCompletableFuture.get();
//						//threshold += 5;
//
//					}catch (Exception e){
//						e.printStackTrace();
//					}
//				}
//			}
//		}
//		if(bigEntry!=null){
//			continueMigrateBigEntry();
//			return;
//		}
		loadBanlancer.setTaskCount(reConfigExecutionVertex.length);
//		if(loadBanlancer.migratePlan.isFinish()){
		this.currentSplitNum = computeCurrentSplitNum();
		loadBanlancer.computeMigratePlan(currentSplitNum);
//		}
		loadBanlancer.migratePlan.printInfo();
		List<MigratePlanEntry> list = loadBanlancer.migratePlan.allRound();
		for(MigratePlanEntry entry:list){
//			if(entry.isBigEntry()){
//				bigEntry = entry;
//				continueMigrateBigEntry();
//				return;
//			}
			try {
				CompletableFuture<Acknowledge> acknowledgeCompletableFuture;
				Set<ExecutionAttemptID> tmp = new HashSet<>();
				tmp.add(reConfigExecutionVertex[entry.getSourceTask()].getCurrentExecutionAttempt().getAttemptId());
				tmp.add(reConfigExecutionVertex[entry.getTargetTask()].getCurrentExecutionAttempt().getAttemptId());
				for (int batch = 0; batch < currentSplitNum; ++batch) {
					acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
						tmp,
						version.addAndGet(1));
					partitionManager.migrte(
						entry,
						batch,
						version.get());
					acknowledgeCompletableFuture.get();
					//Thread.sleep(5000);
				}
				keyGroupTaskMap.put(entry.getKeyGroupIndex(), entry.getTargetTask());//迁移之后需要修改对应的键组映射关系
				// clean阶段
				cleanStage();
				loadBanlancer.clear();
				//threshold += 5;

			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}

	/**
	 * 执行clean阶段
	 */
	public void cleanStage() throws ExecutionException, InterruptedException {
		Set<ExecutionAttemptID> tmp = new HashSet<>();
		CompletableFuture<Acknowledge> acknowledgeCompletableFuture;
		tmp.clear();
		for(ExecutionVertex upstream:upStreamExecutionVertex){
			tmp.add(upstream.getCurrentExecutionAttempt().getAttemptId());
		}
		for(ExecutionVertex reConfig:reConfigExecutionVertex){
			tmp.add(reConfig.getCurrentExecutionAttempt().getAttemptId());
		}
		acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
			tmp,
			version.addAndGet(1));
		partitionManager.clean(version.get());
		acknowledgeCompletableFuture.get();
	}

	/**
	 * 迁移大批次状态，一次迁移不完就下次再迁移
	 */
//	public void continueMigrateBigEntry(){
//		long stateSize = bigEntry.getStateSize();
//		int splitNum = bigEntry.getSplitNum();
//		long perBatchSize = stateSize/splitNum;
//		long haveMigrateSize = 0;
//
//		CompletableFuture<Acknowledge> acknowledgeCompletableFuture;
//		Set<ExecutionAttemptID> tmp = new HashSet<>();
//		tmp.add(reConfigExecutionVertex[bigEntry.getSourceTask()].getCurrentExecutionAttempt().getAttemptId());
//		tmp.add(reConfigExecutionVertex[bigEntry.getTargetTask()].getCurrentExecutionAttempt().getAttemptId());
//		try {
//			while (haveMigrateSize + perBatchSize < MigratePlan.MAX_MIGRATE_STATE_SIZE
//				&& bigEntry.getBatch() < splitNum) {// 每次迁移状态量不能超过200Mb，否则下次再迁移
//				acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
//					tmp,
//					version.addAndGet(1));
//				System.out.println("big Entry batch:"+bigEntry.getBatch());
//				partitionManager.migrte(
//					bigEntry,
//					bigEntry.getBatch(),
//					version.get());
//				acknowledgeCompletableFuture.get();
//				bigEntry.addBatch();
//				haveMigrateSize += perBatchSize;
//			}
//			System.out.println("this epoch finish");
//
//			if(bigEntry.getBatch() == splitNum){
//				bigEntry = null;
//				tmp.clear();
//				for(ExecutionVertex upstream:upStreamExecutionVertex){
//					tmp.add(upstream.getCurrentExecutionAttempt().getAttemptId());
//				}
//				for(ExecutionVertex reConfig:reConfigExecutionVertex){
//					tmp.add(reConfig.getCurrentExecutionAttempt().getAttemptId());
//				}
//				acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
//					tmp,
//					version.addAndGet(1));
//				partitionManager.clean(version.get());
//				acknowledgeCompletableFuture.get();
//				loadBanlancer.clear();
//				loadBanlancer.migratePlan.clear();
//			}
//
//		}catch (Exception e){
//			e.printStackTrace();
//		}
//	}

	public void mayBeRescale(long tmpID){
		if(tmpID >= 700L && !triggerRescale){
			triggerRescale = true;
			int targetIndex = 0;
			for(Map.Entry<Integer, Integer> entry: keyGroupTaskMap.entrySet()){
				if(entry.getValue()==2){
					System.out.println("keyGroup:"+entry.getKey()+" from:"+entry.getValue()+" migrate to:"+targetIndex%2);
					try {
						CompletableFuture<Acknowledge> acknowledgeCompletableFuture;
						Set<ExecutionAttemptID> tmp = new HashSet<>();
						tmp.add(reConfigExecutionVertex[entry.getValue()].getCurrentExecutionAttempt().getAttemptId());
						tmp.add(reConfigExecutionVertex[targetIndex%2].getCurrentExecutionAttempt().getAttemptId());
						for (int batch = 0; batch < currentSplitNum; ++batch) {
							acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
								tmp,
								version.addAndGet(1));
							partitionManager.migrte(
								entry.getValue(),
								targetIndex%2,
								entry.getKey(),
								batch,
								currentSplitNum,
								version.get());
							acknowledgeCompletableFuture.get();
							//Thread.sleep(5000);
						}
						keyGroupTaskMap.put(entry.getKey(), targetIndex%2);//迁移之后需要修改对应的键组映射关系
						// clean阶段
						cleanStage();
						targetIndex++;
						//threshold += 5;

					}catch (Exception e){
						e.printStackTrace();
					}
				}
			}
			rescaleManager.rescalse("ReConfig", 2);
		}
	}

	public void setScheduler(SchedulerNG schedulerNG) {
		this.rescaleManager.setSchedulerNG(schedulerNG);
	}

	private int computeCurrentSplitNum() {
		// TODO 网络模块
		return 4;
	}

	/**
	 * 修改并行度之后更新reconfig vertex
	 * @param reConfigExecutionVertex	修改并行度的重配置节点集
	 */
	public void updateReConfigVertex(ExecutionVertex[] reConfigExecutionVertex){
		System.out.println("reConfigToWait:"+reConfigToWait);
		this.reConfigExecutionVertex = reConfigExecutionVertex;
		this.reConfigToWait = reConfigExecutionVertex.length;
	}

	class QueryState implements Runnable{

		//int i=0;

		@Override
		public void run() {
			try {
				long tmp = version.addAndGet(1);
//				if(tmp > 300L){
//					CompletableFuture<Acknowledge> acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
//						reConfigAttemptSet,
//						tmp);
//					final Execution[] executions = getTriggerExecutions();
//					for(Execution execution:executions){
//						execution.triggerReConfig(
//							new ReConfigSignal(
//								ReConfigSignal.ReConfigSignalType.TEST,
//								tmp
//							)
//						);
//					}
//					currentTrigger.cancel(false);
//					return;
//				}
				CompletableFuture<Acknowledge> acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
					reConfigAttemptSet,
					tmp);
				final Execution[] executions = getTriggerExecutions();
				for(Execution execution:executions){
					execution.triggerReConfig(
						new ReConfigSignal(
							ReConfigSignal.ReConfigSignalType.PERIOD_DETECT,
							tmp
						)
					);
				}
				//long start = System.currentTimeMillis();
				acknowledgeCompletableFuture.get();
//				System.out.println("before rebalance");
//				mayBeReBalance();
//				System.out.println("after rebalance");
//				mayBeRescale(tmp);
//				System.out.println("tmp:"+tmp);
//				System.out.println("now map:"+keyGroupTaskMap);
				Set<ExecutionAttemptID> tmpp = new HashSet<>();
				tmpp.add(reConfigExecutionVertex[0].getCurrentExecutionAttempt().getAttemptId());
				tmpp.add(reConfigExecutionVertex[1].getCurrentExecutionAttempt().getAttemptId());
				acknowledgeCompletableFuture = reConfigResultCollector.startNewRescaleAction(
					tmpp,
					version.addAndGet(1));
				partitionManager.migrte(0, 1, 0, 0, 1, version.get());
				acknowledgeCompletableFuture.get();

				cleanStage();

				//System.out.println("cost time:"+(System.currentTimeMillis()-start));
			}catch (Exception e){
				e.printStackTrace();
			}
		}
	}

}
