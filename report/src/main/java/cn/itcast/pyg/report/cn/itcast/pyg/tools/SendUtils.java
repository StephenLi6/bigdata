package cn.itcast.pyg.report.cn.itcast.pyg.tools;

import cn.itcast.pyg.report.cn.itcast.pyg.bean.Result;
import com.alibaba.fastjson.JSON;
import okhttp3.*;

import java.io.IOException;

public class SendUtils {

    /**
     * 发送数据
     * @param url 发送哪
     * @param data 发送什么数据
     */
    public static void send(String url, String data) {
        //创建客户端
        OkHttpClient client = new OkHttpClient();


        //创建请求对象

        RequestBody requestBody = new FormBody.Builder()
                .add("data", data)
                .build();


        Request request = new Request.Builder()
                .url(url)
                .post(requestBody)
                .build();


        //使用客户端发起请求
        client.newCall(request).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                //失败的时候干嘛?
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                //请求成功你要干嘛
                String resultStr = response.body().string();
                Result result = JSON.parseObject(resultStr, Result.class);
                if (result.getCode() == 200){
                    //正常发送到Kafka
                    System.out.println(result.getMsg());
                }else if (result.getCode() == 302) {
                    System.out.println(result.getData());
                }
            }
        });
    }
}
