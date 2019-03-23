package com.theshy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/3/1810:00
 * com.theshybigdata
 */
public class EsDay01Test {
    private static final String HOST = "node1";
    private static final String CLUSTER_NAME = "myes";
    private static final Integer PORT = 9301;
    private TransportClient client;
    //json格式对象转换器
    ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void init() throws Exception{
        Settings settings = Settings.builder().put("cluster.name",CLUSTER_NAME).build();
        client = new PreBuiltTransportClient(settings);
        TransportAddress address = new TransportAddress(InetAddress.getByName(HOST),PORT);
        client.addTransportAddress(address);
        System.out.println("初始化结束");

    }
    @After
    public void close(){
        if (client!=null) {
            client.close();
            System.out.println("关闭连接");
        }
    }

    @Test
    public void testCreateIndex(){
        client.admin().indices().prepareCreate("blog3").get();
    }

    @Test
    public void testAddDoc(){
        Map<String,String> sourceMap = new HashMap<String, String>();
        sourceMap.put("id", "3");
        sourceMap.put("title","3ElasticSearch是一个基于Lucene的搜索服务器。");
        sourceMap.put("content","3它提供了一个分布式多用户能力的全文搜索引擎，基于RESTful web接口。Elasticsearch是用Java开发的，并作为Apache许可条款下的开放源码发布，是当前流行的企业级搜索引擎。设计用于云计算中，能够达到实时搜索，稳定，可靠，快速，安装使用方便。");
        client.prepareIndex("blog3", "article","4").setSource(sourceMap).get();

    }

    @Test
    public void testCreateMapping() throws Exception{
        //构建一个json内容构造器
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject() //{
                .startObject("article2")
                .startObject("properties")
                .startObject("id")
                .field("type","long")
                .endObject()
                .startObject("title")
                .field("type","text")
                .field("store","true")
                .field("analyzer","ik_max_word")
                .endObject()
                .startObject("content")
                .field("type","text")
                .field("store","true")
                .field("analyzer","ik_max_word")
                .endObject()
                .endObject()
                .endObject()
                .endObject(); //}
        PutMappingRequest mapping = Requests.putMappingRequest("blog3").type("article2").source(builder);
        client.admin().indices().putMapping(mapping).get();
    }

    //在特定域上搜索特定字符串,与第一天结果不一致,主要是使用IK分词
    @Test
    public void testStringQuery() throws Exception {
        //设置查询条件（QueryBuilders.matchAllQuery()：查询所有）
        QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("搜索");
        //查到数据
        //QueryStringQueryBuilder queryStringQueryBuilder = QueryBuilders.queryStringQuery("搜寻").field("title"); //没有查到
        query(queryStringQueryBuilder);
    }

    //执行查询并打印结果
    private void query(QueryBuilder query) {
        System.out.println(query);
        SearchResponse searchResponse = client.prepareSearch("blog3").setTypes("article2")
                .setQuery(query).get(); //指定标题上搜索 没有结果

        //处理结果
        SearchHits hits = searchResponse.getHits(); //获得命中目标，即查询到了多少个对象
        System.out.println("共查询" + hits.getTotalHits() + "条");
        Iterator<SearchHit> ite = hits.iterator();
        while (ite.hasNext()) {
            SearchHit searchHit = ite.next();
            System.out.println(searchHit.getSourceAsString());
            System.out.println(searchHit.getSourceAsMap().get("title"));
            try {
                //json格式对象转换器
                Article article = objectMapper.readValue(searchHit.getSourceAsString(), Article.class);
                System.out.println(article);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //关闭资源
        client.close();
    }

}
