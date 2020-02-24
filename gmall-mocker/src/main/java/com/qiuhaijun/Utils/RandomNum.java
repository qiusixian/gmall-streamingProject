package com.qiuhaijun.Utils;

/**
 * @ClassName RandomNum
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2020/2/17 20:04
 * @Version 1.0
 **/
import java.util.Random;


public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return   fromNum+ new Random().nextInt(toNum-fromNum+1);
    }
}

