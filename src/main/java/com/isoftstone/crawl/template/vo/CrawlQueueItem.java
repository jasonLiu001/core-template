package com.isoftstone.crawl.template.vo;

/**
 * Created by donegal on 2015/5/6.
 */
public class CrawlQueueItem {
    private String url;
    private String type;
    private String templateID;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTemplateID() {
        return templateID;
    }

    public void setTemplateID(String templateID) {
        this.templateID = templateID;
    }
}
