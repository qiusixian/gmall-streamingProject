package com.qiuhaijun.gmalllogger.controller;

/**
 * @ClassName LoggerController
 * @Description TODO
 * @Author qiuhaijun
 * @Date 2020/2/18 20:21
 * @Version 1.0
 **/

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qiuhaijun.constants.GmallConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/**
 * @Slf4j是我们装的lombok下的包，它能自动为我们创建一个名为log的对象，通过这个对象我们直接可以指定日志类型（4种类型）来输出
 *
 * @RestController = @Controller + @ResponseBody，
 * 使用@Controller注解的时候，就意味着你要使用前后端结合的方式来开发了，这个时候Controller里面的方法返回值有3种：
 *
 * 1、字符串，代表视图名称
 *
 * 2、ModelAndView，需要渲染数据的视图
 *
 * 3、void，通过HttpServetResponse跳转或返回，（转发、重定向、返回）
 *
 * 还有使用@ResponseBody注解的，此注解是使用在方法上的，作用是使方法返回时不走视图解析器，直接将返回值通过相应的转换器写入HTTP相应中。
 *
 * 后来Spring又提供了@RestController，此注解的作用是@Controller和@ResponseBody的结合。
 *
 * */
@RestController
@Slf4j
public class LoggerController {



    /*@GetMapping("test1")
    //前后端交互参数，通过注解 @GetMapping（URL），实现网页与我们的代码交互，表示拦截地址URL里面的内容。
    *//**
     * 我们在网页中输入localhost：8080/test1，那么就会调用我们注解@GetMapping("test1")下面的这个方法，然后再页面上显示调用这个方法的结果。
     * *//*
    public String test() {
        return "success";
    }

    @GetMapping("test2")
    *//**
     * 在页面上通过参数传入到我们方法中的形参。需要用注解@RequestParam("aa")，括号里面是页面上传的参数名，
     *
     * 下面的@RequestParam("aa")，其中aa表示页面上要传的参数名，后面String a表示我们方法中的形参类型和形参名。
     * 通过页面上的参数aa将值传递给方法中的形参a，然后直接在方法中就可以用了，由于有@RestController注解，方法的结果会返回到页面中显示。
     * 在页面地址栏中中输入http://localhost:8080/test2?aa=dfs，最后在页面中就会显示dfs；其中？后面就跟的页面参数。
     * *//*
    public String test(@RequestParam("aa") String a) {
        return a;
    }

    //    @ResponseBody
    @PostMapping("log")
    *//**
     * 这里为啥用@PostMapping注解，因为@GetMapping注解只能拦截页面上的get请求，不能拦截post请求。
     *
     * 这里的拦截地址用的是相对路径，拦截的是http://localhost:8090/log这个路径，由于是相对路径所以直接用log。
     * *//*
    public String logger(@RequestParam("logString") String logStr) {

        //System.out.println(logStr);//测试接收到web端口的数据*/


        //下面是测试数据写到kafka中。并打印到日志文件中

            /**
             * @Autowired表示自动注入，自动到application.properties中找到相应的配置信息，自动构建对象，不用自己再newkafkaTemplate对象。
             *
             * 我们只需要在application.properties中配置好相应的信息即可。
             *
             * 注意：下面的kafkaTemplate看到是红色的错误，实际上是没有问题的。
             * */
        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

    @PostMapping("log")
    public String logger(@RequestParam("logString") String logStr) {
        //0.添加时间戳字段（添加后台服务器的时间）
        //先转成json格式，添加时间戳，然后再转回成String类型
        JSONObject jsonObject = JSON.parseObject(logStr);
        jsonObject.put("ts", System.currentTimeMillis());

        String tsJson = jsonObject.toString();

        //1.使用log4j打印日志到控制台及文件
        log.info(tsJson);

        //2.使用Kafka生产者将数据发送到Kafka集群（根据数据类型start和event分成两个主题）
        if (tsJson.contains("startup")) {
            //将数据发送至启动日志主题
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, tsJson);
        } else {
            //将数据发送至事件日志主题
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, tsJson);
        }

        return "success";
    }

}
