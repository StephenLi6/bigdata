package cn.itcast.pyg.report.cn.itcast.pyg.bean;

public class Message {
    //时间戳
    private Long timeStamp;
    //消息数量
    private Integer count;
    //消息体
    private String data;

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return "Message{" +
                "timeStamp=" + timeStamp +
                ", count=" + count +
                ", data='" + data + '\'' +
                '}';
    }
}
