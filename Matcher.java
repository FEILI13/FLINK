package org.apache.flink.runtime.checkpoint.Checkpoint_storage;


import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.hmx_bishe.Operator_StateSize;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Matcher {

	public int nodes; //execution数量(execution里可以有多个operator)
	public float[][] bandWidth; //节点间带宽
	public Map<OperatorID,Operator_StateSize> stateSize; //算子状态大小，key ：OperatorID, Operator_StateSize内部是每个子任务的状态大小

	private Map<String,Operator_StateSize> IPstateSize;
	public int IPNodes;//节点数量
	public String[] IP;//节点IP
	public Map<ExecutionVertex,String> executionIP;
	public Map<String,Integer> IPOrder;
	public int score[][];
	public Matcher(int nodes, int IPNodes, Set<String> Ip, Map<ExecutionVertex,String> OperatorIP, Map<OperatorID, Operator_StateSize> opStateSize){
		this.nodes = nodes; //execution数量(execution里可以有多个operator)
		this.IPNodes = IPNodes;//节点数量
		this.executionIP = OperatorIP; //Execution：IP
		this.IP = new String[IPNodes]; //各个节点IP
		int i=0;
		for(String str : Ip){ //
			IP[i] = str;
			i++;
		}
		IPOrder = new HashMap<>();
		for(int j = 0; j< IPNodes; j++){
			IPOrder.put(IP[j],j); //为每个IP写编号
		}
		stateSize = opStateSize; //算子状态大小，key ：OperatorID, Operator_StateSize内部是每个子任务的状态大小
		//初始化算子状态大小，最初默认初始化都为 1
		for(Map.Entry<ExecutionVertex,String> entry: executionIP.entrySet()){
			List<OperatorIDPair> opid = entry.getKey().getJobVertex().getJobVertex().getOperatorIDs();
			int subIndex = entry.getKey().getParallelSubtaskIndex();
			for(OperatorIDPair op:opid){
				OperatorID oid = op.getGeneratedOperatorID();
				Operator_StateSize os = stateSize.get(oid);
				if(os==null){
					os = new Operator_StateSize();
					os.size.put(subIndex,1l);
					stateSize.put(oid,os);
				}
				else{
					os.size.put(subIndex,1l);
				}
			}
		}
	}

	public void computeBandWidth(String[] IP,int IpNodes){

	}

	private void getIPStateSize() {
		for (Map.Entry<OperatorID, Operator_StateSize> entry : stateSize.entrySet()) {
			String IP = entry.getKey().getIP();
			Operator_StateSize opStateSize = entry.getValue();
			IPstateSize.put(IP,IPstateSize.get(IP) + opStateSize);
		}
	}
	private void getDistance() {

	}
	private void getNearMatchScore() {

	}
	private void getFarMatchScore() {

	}
	//计算每个节点上的算子应该把状态发送到哪些节点 IP-IP(源-目的)
	public Map<String,String> compute(Map<OperatorID, Operator_StateSize> opState_size){
		KMRunner runner = new KMRunner();

		computeBandWidth(IP,IPNodes); //计算节点间带宽
		getIPStateSize(); //计算节点的状态大小
		getDistance(); //计算节点间距离
		getNearMatchScore(); //计算近点匹配的得分
		HashMap<Integer,Integer> match1 = runner.run(score);
		getFarMatchScore(); //计算远点匹配的得分
		HashMap<Integer,Integer> match2 = runner.run(score);
		return null;
	}


}
