package org.apache.flink.runtime.reConfig.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

public class HttpMetricClient {
    String jobManagerIP;
    public HttpMetricClient(String jobManagerIP) {
        this.jobManagerIP = jobManagerIP;
    }

    public Map GetNetworkMetric() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String,String> msgMap = new HashMap<>();
        msgMap.put("method", "get");
        objectMapper.writeValueAsString(msgMap);
        String msg = objectMapper.writeValueAsString(msgMap);
        try {
            Socket socket = new Socket(jobManagerIP,8088);
            PrintWriter pw = new PrintWriter(socket.getOutputStream());
            BufferedReader is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            pw.println(msg);
            pw.flush();
            String line = is.readLine();
            //System.out.println(line);
            pw.close();
            is.close();
            socket.close();
            Map metric = objectMapper.readValue(line,Map.class);
            return metric;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
    public String sendGetRequest(String URL) {
        try {
            URL url = new URL(URL);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");

            int responseCode = connection.getResponseCode();

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();

            //System.out.println("Response Code: " + responseCode);
            //System.out.println("Response: " + response.toString());
            return response.toString();
        } catch (Exception e) {
            e.printStackTrace();

        }
        return "null";
    }

    public String[] getTaskManagerMetrics() {
        String response = sendGetRequest("http://" + jobManagerIP + ":8081/taskmanagers");
        String[] Metrics = new String[10];
        int idx = 0;
        while(response.contains("id")) {
            response = response.substring(response.indexOf("id") + 5);
            String id = response.substring(0,response.indexOf("\""));
            Metrics[idx] = sendGetRequest("http://" + jobManagerIP + ":8081/taskmanagers/" + id);
            System.out.println(Metrics[idx]);
            idx++;
        }
        return Metrics;
    }

    public String[] getJobVerticesMetrics(String jobId, String metric) {
        String response = sendGetRequest("http://" + jobManagerIP + ":8081/jobs/" + jobId);
        String vertices = response.substring(response.indexOf("vertices"),response.indexOf("plan"));
        String[] Metrics = new String[10];
        int idx = 0;
        while(vertices.contains("id")) {
            vertices = vertices.substring(vertices.indexOf("id") + 5);
            String id = vertices.substring(0,vertices.indexOf("\""));
            vertices = vertices.substring(vertices.indexOf("name") + 7);
            String name = vertices.substring(0,vertices.indexOf("\""));
            Metrics[idx] = name + ": " + sendGetRequest("http://" + jobManagerIP + ":8081/jobs/" + jobId + "/vertices/"
                + id + "/metrics?get=" + metric);
            System.out.println(Metrics[idx]);
            idx++;
        }
        return Metrics;
    }

//    public static void main(String[] args) throws IOException, InterruptedException {
//
//        NetworkMetricStarter server = new NetworkMetricStarter("C:\\Users\\feili\\Desktop\\JobManagerNet.py");
//        Thread t = new Thread(server);
//        t.start();
//
//        HttpMetricClient httpMetricClient = new HttpMetricClient("localhost");
//        Map metric = httpMetricClient.GetNetworkMetric();
//        metric.forEach((ip1, value1) -> {
//            Map ipTable = (Map) value1;
//            ipTable.forEach((ip2, value2) -> {
//                Map metricTable = (Map) value2;
//                System.out.println(ip1 + " -> " + ip2 + " " + metricTable.toString());
//            });
//        });
//    }

}
