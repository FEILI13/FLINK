package org.apache.flink.runtime.checkpoint.Checkpoint_storage;

import java.util.HashMap;

public class Matcher {
    private int[][] cost;
    private void nearMatchInit(HashMap<String,String> match) {
        cost = null;
    }
    private void FarMatchInit(HashMap<String,String> match) {
        cost = null;
        if(match != null) {
            for( entry: match){

            }
        }
    }
    
    public HashMap<String,String> match(int level) {
        HashMap<String,String> res = new HashMap<>();
        KMRunner runner = new KMRunner();
        nearMatchInit();
        HashMap<Integer,Integer> match = runner.run(cost);
        if(level == 1) {
            return res;
        }
        FarMatchInit(match);
        runner.run(cost);
        return res;
    }
}
