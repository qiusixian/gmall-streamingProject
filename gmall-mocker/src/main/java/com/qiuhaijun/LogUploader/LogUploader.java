package com.qiuhaijun.LogUploader;

/**
 * @ClassName LogUploader
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2020/2/17 20:07
 * @Version 1.0
 **/

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

//LogUploader：通过http方法把我们自己造的数据发送到采集系统的web端口，
// 然后在gmall-logger模块中controller类来这个web端口通过参数的形式把数据采集到本地文件夹中，并打印到控制台中。
public class LogUploader {

    public static void sendLogStream(String log){
        try{
            //不同的日志类型对应不同的URL
            /**
             * 在本地做测试，默认用本地的tomocat端口。我们改成8090端口，这个里改了端口，那么在application.properties中也要把这个端口添加进去
             * */
            //URL url  =new URL("http://localhost:8090/log");

            //在Linux上单机测试时，我们需要将URL改成Linux上的路径。相当于web端口起在Linux系统上。
            //URL url  =new URL("http://hadoop102:8090/log");

            //在Linux上集群测试时，我们通过代理服务器URL端口进行测试。nginx端口默认是80，而且可以省略。
            URL url  =new URL("http://hadoop102:80/log");


            HttpURLConnection conn = (HttpURLConnection) url.openConnection();

            //设置请求方式为post
            conn.setRequestMethod("POST");

            //时间头用来供server进行时钟校对的
            conn.setRequestProperty("clientTime",System.currentTimeMillis() + "");

            //允许上传数据
            conn.setDoOutput(true);

            //设置请求的头信息,设置内容类型为JSON
            conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

            System.out.println("upload" + log);

            //输出流
            OutputStream out = conn.getOutputStream();
            out.write(("logString="+log).getBytes());
            out.flush();
            out.close();
            int code = conn.getResponseCode();
            System.out.println(code);
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}

