package cn.itcast.pyg.report.cn.itcast.pyg.controller;


import cn.itcast.pyg.report.cn.itcast.pyg.bean.Message;
import cn.itcast.pyg.report.cn.itcast.pyg.bean.Result;
import com.alibaba.fastjson.JSON;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
@RequestMapping("report")
public class ReportController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("sendKafka")
    public Result sendKafka(String data) {
        Message message = new Message();
        message.setCount(1);
        message.setTimeStamp(new Date().getTime());
        message.setData(data);

        //使用FastJson将message转换为json字符串
        String json = JSON.toJSONString(message);

        Result result = new Result();
        try {
            //将消息发送到Kafka
            kafkaTemplate.send("pyg", json);
            //根据Kafka的发送情况,进行结果返回
            result.setCode(200);
            result.setMsg("success");
        } catch (Exception e) {
            //根据Kafka的发送情况,进行结果返回
            result.setCode(302);
            result.setMsg("平台正在维护,请稍后重试");
            result.setData(e.toString());
        }
        return result;
    }

}
