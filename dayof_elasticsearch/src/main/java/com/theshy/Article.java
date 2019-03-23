package com.theshy;

import org.springframework.data.elasticsearch.annotations.Document;

/*
 * The Best Or Nothing
 * Desinger:TheShy
 * Date:2019/3/1911:05
 * com.theshybigdata
 */
public class Article {
    private Integer id;
    private String title;
    private String content;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
