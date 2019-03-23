package cn.itcast.pyg.report.cn.itcast.pyg.controller;


import cn.itcast.pyg.report.cn.itcast.pyg.bean.Message;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("report")
public class ReportController {


    @RequestMapping("sendKafka")
    public void sendKafka(String data) {
        Message message = new Message();
        message.setCount(1);
        message.setTimeStamp(new Date().getTime());
        message.setData(data);
        System.out.println(message);
        //将消息发送到Kafka

        //根据Kafka的发送情况,进行结果返回

    }

}
