package org.apache.flink.runtime.checkpoint.Checkpoint_storage;

import java.util.Arrays;
import java.util.HashMap;

public class KMRunner {
    private final int INF = Integer.MAX_VALUE;
    private int N; //顶点数
    private int[][] weight;
    private int[] lx;
    private int[] ly;
    private boolean[] visitX;
    private boolean[] visitY;
    private int[] match;
    private int[] slack;

    private void init(int[][] cost) {
        N = cost.length;
        weight = new int[N][N];
        for (int i = 0; i < cost.length; i++) {
            weight[i] = Arrays.copyOf(cost[i], cost[i].length);
        }
        match = new int[N];
        lx = new int[N];
        ly = new int[N];
        visitX = new boolean[N];
        visitY = new boolean[N];
        slack = new int[N];

        Arrays.fill(match, -1);
        Arrays.fill(ly, 0);
        for (int i = 0; i < N; i++) {
            lx[i] = -INF;
            for (int j = 0; j < N; j++) {
                if (weight[i][j] > lx[i]) {
                    lx[i] = weight[i][j];
                }
            }
        }
    }

    private boolean hungary(int x) {
        visitX[x] = true;
        for (int y = 0; y < N; y++) {
            if (visitY[y]) {
                continue;
            }
            int t = lx[x] + ly[y] - weight[x][y];//t=0,在相等子图中找匹配
            if (t == 0) {
                visitY[y] = true;
                if (match[y] == -1 || hungary(match[y])) {
                    match[y] = x;
                    return true;
                }
            } else if (slack[y] > t) {
                slack[y] = t; //寻找增广路径时,顺便将slack值算出
            }
        }
        return false;
    }

    private HashMap<Integer, Integer> match() {
        for (int x = 0; x < N; x++) {
            Arrays.fill(slack, INF);
            while (true) {
                Arrays.fill(visitX, false);
                Arrays.fill(visitY, false);
                if (hungary(x)) {
                    break;//找到增广轨,退出
                }
                int d = INF;
                for (int i = 0; i < N; i++) {
                    if (!visitY[i] && d > slack[i]) {
                        d = slack[i];
                    }
                }
                for (int i = 0; i < N; i++) {
                    if (visitX[i])
                        lx[i] -= d;//修改x的顶标
                }
                for (int i = 0; i < N; i++) {
                    if (visitY[i])
                        ly[i] += d;
                    else
                        slack[i] -= d;//修改顶标后,不在路径中的Y点的顶标要减d
                }
            }
        }
        //返回匹配结果
        HashMap<Integer, Integer> res = new HashMap<>();
        for (int i = 0; i < N; i++) {
            if (match[i] > -1) {
                res.put(i,match[i]);
            }
        }
        return res;
    }

    public HashMap<Integer, Integer> run(int[][] cost) {
        init(cost);
        return match();
    }
}