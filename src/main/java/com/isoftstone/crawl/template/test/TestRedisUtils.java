package com.isoftstone.crawl.template.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSON;
import com.isoftstone.crawl.template.vo.CrawlQueueItem;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.isoftstone.crawl.template.global.Constants;
import com.isoftstone.crawl.template.impl.ParseResult;
import com.isoftstone.crawl.template.impl.Selector;
import com.isoftstone.crawl.template.impl.SelectorFilter;
import com.isoftstone.crawl.template.impl.SelectorFormat;
import com.isoftstone.crawl.template.impl.SelectorIndexer;
import com.isoftstone.crawl.template.impl.TemplateFactory;
import com.isoftstone.crawl.template.impl.TemplateResult;
import com.isoftstone.crawl.template.utils.MD5Utils;
import com.isoftstone.crawl.template.utils.RedisUtils;

public class TestRedisUtils {
    static ObjectMapper objectMapper = null;
    static JsonGenerator jsonGenerator = null;

    public static void ccgpTest() {
        String url = "http://search.ccgp.gov.cn/dataB.jsp?searchtype=1&page_index=1&bidSort=1&buyerName=&projectId=&pinMu=1&bidType=1&dbselect=bidx&kw=&start_time=2014%3A10%3A27&end_time=2014%3A11%3A03&timeType=2&displayZone=&zoneId=&agentName=";
        String encoding = "utf8";
        String templateGuid = MD5Utils.MD5(url);
        String filePath = "D:/Develop/resource/标讯库搜索_中国政府采购�?1.html";
        TemplateResult t = createPageForCCGP(templateGuid);

        ParseResult r = TemplateResultTest(url, encoding, filePath, t, "list");
        System.out.println(r.toString());

        url = "http://www.ccgp.gov.cn/cggg/zygg/gkzb/201411/t20141103_4689829.htm";
        filePath = "D:/Develop/resource/中国医学科学院阜外心�?管病医院（中国医学科学院心血管病研究�?）新大楼�?办费项目第四十五次采购招标公�?.html";
        r = TemplateResultTest(url, encoding, filePath, t, "news");
        System.out.println(r.toString());
    }

    public static void ccgpTestRedis() {
        String url = "http://search.ccgp.gov.cn/dataB.jsp?searchtype=1&page_index=1&bidSort=1&buyerName=&projectId=&pinMu=1&bidType=1&dbselect=bidx&kw=&start_time=2014%3A10%3A27&end_time=2014%3A11%3A03&timeType=2&displayZone=&zoneId=&agentName=";
        String encoding = "utf8";
        String templateGuid = MD5Utils.MD5(url);
        String filePath = "D:/Develop/resource/标讯库搜索_中国政府采购�?1.html";
        TemplateResult t = createPageForCCGP(templateGuid);
        RedisUtils.setTemplateResult(t, templateGuid, 0);

        byte[] input = readTextFile(filePath, encoding);
        ParseResult r = TemplateFactory.process(input, encoding, url, 0);
        System.out.println(r.toString());

        url = "http://www.ccgp.gov.cn/cggg/zygg/gkzb/201411/t20141103_4689829.htm";
        filePath = "D:/Develop/resource/中国医学科学院阜外心�?管病医院（中国医学科学院心血管病研究�?）新大楼�?办费项目第四十五次采购招标公�?.html";
        input = readTextFile(filePath, encoding);
        r = TemplateFactory.process(input, encoding, url, 0);
        System.out.println(r.toString());
    }

    public static void createZhongguoxintuo() {
        String seedUrl = "http://www.zhongguoxintuo.com/xtxw/";

        TemplateResult template = new TemplateResult();
        String templateGuid = MD5Utils.MD5(seedUrl);
        template.setType(Constants.TEMPLATE_LIST);
        template.setTemplateGuid(templateGuid);
        // list template
        List<Selector> list = new ArrayList<Selector>();
        Selector selector = new Selector();
        SelectorIndexer index = new SelectorIndexer();

        selector.setType(Constants.SELECTOR_CONTENT);
        selector.setName(Constants.CONTENT_OUTLINK);
        index.setType("jsoup");
        index.setValue("div.list dl dt a");
        index.setAttribute("href");
        selector.setIndexer(index);
        list.add(selector);

        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_PAGITATION);
        selector.setName(Constants.PAGINATION_OUTLINK);
        index.setType("jsoup");
        index.setValue("div.page a");
        index.setAttribute("href");
        selector.setIndexer(index);
        list.add(selector);
        template.setList(list);

        // news template
        List<Selector> news = new ArrayList<Selector>();
        // title
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("title");
        index.setType("jsoup");
        index.setValue("div.title");
        index.setAttribute("text");
        selector.setIndexer(index);
        news.add(selector);
        // content
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("content");
        index.setType("jsoup");
        index.setValue("div.cont_c");
        index.setAttribute("text");
        selector.setIndexer(index);
        news.add(selector);
        // tstamp
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("tstamp");
        index.setType("jsoup");
        index.setValue("div.ly");
        index.setAttribute("text");
        selector.setIndexer(index);
        List<SelectorFilter> filters = new ArrayList<SelectorFilter>();
        SelectorFilter filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("\\d{4}-\\d{2}-\\d{2}");
        filters.add(filter);
        List<SelectorFormat> formats = new ArrayList<SelectorFormat>();
        SelectorFormat format = new SelectorFormat();
        format.setType(Constants.FORMAT_DATE);
        format.setValue("yyyy-MM-DD");
        formats.add(format);
        news.add(selector);
        template.setNews(news);

        RedisUtils.setTemplateResult(template, templateGuid, 0);

        String filePath = "e:/test2.html";
        String encoding = "GB2312";

        byte[] input = readTextFile(filePath, encoding);

        ParseResult r = TemplateFactory.process(input, encoding, seedUrl, 0);
        System.out.println(r.toString());
    }

    /**
     * @param guid
     * @return
     */
    /**
     * @param guid
     * @return
     */
    /**
     * @param guid
     * @return
     */
    public static TemplateResult createPageResult(String guid) {
        TemplateResult pageResult = new TemplateResult();

        List<Selector> list = new ArrayList<Selector>();
        Selector selector = new Selector();
        SelectorIndexer index = new SelectorIndexer();

        selector.setType("content");
        selector.setName("content_outlink");
        index.setType("jsoup");
        index.setValue("div.table_1 table tbody tr td h2 a");
        index.setAttribute("href");
        selector.setIndexer(index);

        List<Selector> labels = new ArrayList<Selector>();
        Selector label = new Selector();
        label.setName("article_industry");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("div.table_1 table tbody tr td:nth-child(2)");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        List<SelectorFilter> filters = new ArrayList<SelectorFilter>();
        SelectorFilter filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("[");
        filters.add(filter);

        filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("]");
        filters.add(filter);
        label.setFilter(filter);
        labels.add(label);

        label = new Selector();
        label.setName("article_area");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("div.table_1 table tbody tr td:nth-child(3)");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        filters = new ArrayList<SelectorFilter>();
        filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("[");
        filters.add(filter);

        filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("]");
        filters.add(filter);
        label.setFilter(filter);
        labels.add(label);

        label = new Selector();
        label.setName("article_published");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("div.table_1 table tbody tr td:nth-child(4)");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        filters = new ArrayList<SelectorFilter>();
        filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("[");
        filters.add(filter);

        filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("]");
        filters.add(filter);
        label.setFilter(filter);

        List<SelectorFormat> formats = new ArrayList<SelectorFormat>();
        SelectorFormat format = new SelectorFormat();
        format.setType(Constants.FORMAT_DATE);
        format.setValue("yyyy-MM-dd");
        label.setFormat(format);
        labels.add(label);

        selector.setLabel(label);
        list.add(selector);

        selector = new Selector();
        selector.setType("pagination");
        selector.setName("list_outlink");
        index = new SelectorIndexer();
        index.setType("jsoup");
        index.setValue("div.searchPage form a:contains(下一�?)");
        index.setAttribute("href");
        selector.setIndexer(index);
        list.add(selector);

        pageResult.setList(list);

        pageResult.setTemplateGuid(guid);
        pageResult.setType("list");
        return pageResult;
    }

    public static TemplateResult createPageResultForNewsPage(
            TemplateResult pageResult) {
        List<Selector> list = new ArrayList<Selector>();
        Selector selector = new Selector();
        SelectorIndexer index = new SelectorIndexer();

        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("content_outlink");
        index.setType("jsoup");
        index.setValue("ul.vT-srch-result-list-bid li a");
        index.setAttribute("href");
        selector.setIndexer(index);

        List<Selector> labels = new ArrayList<Selector>();
        Selector label = new Selector();
        label.setName("article_industry");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("div.table_1 table tbody tr td:nth-child(2)");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        List<SelectorFilter> filters = new ArrayList<SelectorFilter>();
        SelectorFilter filter = new SelectorFilter();
        filter.setType(Constants.FILTER_REMOVE);
        filter.setValue("[");
        filters.add(filter);
        list.add(selector);

        selector = new Selector();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("article_content");
        index = new SelectorIndexer();
        index.setType("jsoup");
        index.setValue("div.infoClass");
        index.setAttribute("text");
        selector.setIndexer(index);
        list.add(selector);

        selector = new Selector();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("article_metacontent");
        index = new SelectorIndexer();
        index.setType("jsoup");
        index.setValue("div.infoClass");
        index.setAttribute("html");
        selector.setIndexer(index);
        list.add(selector);

        pageResult.setNews(list);
        return pageResult;
    }

    public static TemplateResult createPageForCCGP(String templateGuid) {
        TemplateResult template = new TemplateResult();
        template.setType("list");
        template.setTemplateGuid(templateGuid);
        // list template
        List<Selector> list = new ArrayList<Selector>();
        Selector selector = new Selector();
        SelectorIndexer index = new SelectorIndexer();

        selector.setType(Constants.SELECTOR_CONTENT);
        selector.setName("content_outlink");
        index.setType("jsoup");
        index.setValue("ul.vT-srch-result-list-bid li > a");
        index.setAttribute("href");
        selector.setIndexer(index);
        List<Selector> labels = new ArrayList<Selector>();
        Selector label = new Selector();
        // bidKind lable
        label.setName("bidKind");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("ul.vT-srch-result-list-bid li span strong:nth-child(2)");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        labels.add(label);

        // area lable
        label = new Selector();
        label.setName("area");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("ul.vT-srch-result-list-bid li span a");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        labels.add(label);

        // industry label
        label = new Selector();
        label.setName("industry");
        label.setType(Constants.SELECTOR_LABEL);
        index = new SelectorIndexer();
        index.setValue("ul.vT-srch-result-list-bid li span strong:nth-child(4)");
        index.setAttribute("text");
        index.setType("jsoup");
        label.setIndexer(index);
        labels.add(label);
        selector.setLabel(label);
        list.add(selector);
        template.setList(list);

        // TODO: pagitation
        // template.setList(pagitation);

        // news template
        List<Selector> news = new ArrayList<Selector>();
        selector = new Selector();
        index = new SelectorIndexer();
        // title
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("title");
        index.setType("jsoup");
        index.setValue("div.vT_detail_header h2.tc");
        index.setAttribute("text");
        selector.setIndexer(index);
        news.add(selector);

        // publishDate
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("publishDate");
        index.setType("jsoup");
        index.setValue("span#pubTime");
        index.setAttribute("text");
        selector.setIndexer(index);
        news.add(selector);

        // oriName
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("oriName");
        selector.setValue("中国政府采购�?");
        news.add(selector);

        // mainBodyStr
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("mainBodyStr");
        index.setType("jsoup");
        index.setValue("div.vT_detail_content.w760c");
        index.setAttribute("text");
        selector.setIndexer(index);
        news.add(selector);

        // mainBodyHtml
        selector = new Selector();
        index = new SelectorIndexer();
        selector.setType(Constants.SELECTOR_FEILD);
        selector.setName("mainBodyStr");
        index.setType("jsoup");
        index.setValue("div.vT_detail_content.w760c");
        index.setAttribute("html");
        selector.setIndexer(index);
        news.add(selector);

        template.setNews(news);
        return template;
    }

    public static byte[] readTextFile(String filePath, String encoding) {
        java.io.InputStreamReader inputReader = null;
        java.io.BufferedReader reader = null;
        File f = new File(filePath);
        try {
            inputReader = new java.io.InputStreamReader(
                    new java.io.FileInputStream(f), encoding);
            reader = new java.io.BufferedReader(inputReader);
        } catch (UnsupportedEncodingException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        String s = null;
        try {
            s = reader.readLine();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        StringBuffer sb = new StringBuffer();
        while (s != null) {
            sb.append(s);
            sb.append("\r\n");
            try {
                s = reader.readLine();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        byte[] page = sb.toString().getBytes();
        // System.out.println(sb.toString());
        try {
            reader.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return page;
    }

    public static void main(String[] args) {
        // ccgpTestRedis();
        //createZhongguoxintuo();

        CrawlQueueItem tpItem = new CrawlQueueItem();
        for (int i = 0; i < 100; i++) {
            tpItem.setUrl("the url of " + i);
            RedisUtils.setQueueItem(tpItem);
        }
        while (true) {
            CrawlQueueItem tp = RedisUtils.getQueueItem();
            if (tp == null) {
                break;
            } else {
                System.out.print(JSON.toJSONString(tp));
            }
        }
    }

    public static ParseResult TemplateResultTest(String url, String encoding,
                                                 String filePath, TemplateResult t, String pageType) {
        byte[] input = readTextFile(filePath, encoding);
        return TemplateFactory.localProcess(input, encoding, url, t, pageType);
    }

}
