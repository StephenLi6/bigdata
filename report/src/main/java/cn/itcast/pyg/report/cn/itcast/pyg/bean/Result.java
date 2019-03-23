package cn.itcast.pyg.report.cn.itcast.pyg.bean;

public class Result {

    //响应状态码
    //200 正常
    //301 登录失败
    //302 后台服务器异常
    private int code;

    //提示性的信息
    //对不起,用户名或者密码错误
    //后台工程师正在维护,请稍后重试
    private String msg;

    //携带数据信息
    //查看订单
    private String data;


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Result{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                ", data='" + data + '\'' +
                '}';
    }
}
