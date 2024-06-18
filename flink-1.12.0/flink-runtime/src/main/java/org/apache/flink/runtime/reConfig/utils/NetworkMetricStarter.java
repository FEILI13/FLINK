package org.apache.flink.runtime.reConfig.utils;

import java.io.IOException;

public class NetworkMetricStarter implements Runnable{
    String filePath;
    public NetworkMetricStarter(String filePath) {
        this.filePath = filePath;
    }
    @Override
    public void run() {
        try {
            String command = "python3 " + filePath;
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
