package com.jueee.learnspark.dataanalysis.common;

import java.io.Serializable;

public class AvgCount implements Serializable {
    public int total;
    public int num;
    public AvgCount(int total,int num){
        this.total = total;
        this.num = num;
    }
    public double avg(){
        return total/(double)num;
    }
}