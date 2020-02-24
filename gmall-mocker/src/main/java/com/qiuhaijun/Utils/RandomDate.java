package com.qiuhaijun.Utils;

import java.util.Date;
import java.util.Random;

/**
 * @ClassName RandomDate
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2020/2/17 16:15
 * @Version 1.0
 **/

/**
 * 生成随机日期：
 *
 * */
public class RandomDate {

    private Long logDateTime =0L;
    private int maxTimeStep=0 ;

    public RandomDate (Date startDate , Date  endDate, int num) {
        Long avgStepTime = (endDate.getTime()- startDate.getTime())/num;
        this.maxTimeStep=avgStepTime.intValue()*2;
        this.logDateTime=startDate.getTime();
    }

    public  Date  getRandomDate() {
        int  timeStep = new Random().nextInt(maxTimeStep);
        logDateTime = logDateTime+timeStep;
        return new Date( logDateTime);
    }
}

