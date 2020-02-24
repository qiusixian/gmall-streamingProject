package com.qiuhaijun.Utils;

/**
 * @ClassName RanOpt
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2020/2/17 20:03
 * @Version 1.0
 **/
public class RanOpt<T>{
    private T value ;
    private int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}

