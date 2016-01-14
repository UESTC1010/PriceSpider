import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.RouteProcessor;
import us.codecraft.webmagic.selector.Selectable;

import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.concurrent.atomic.AtomicInteger;

public class JDSpider extends RouteProcessor
{
    private static final String URL_LIST = ".*list\\.jd\\.com.*";
    private static final String URL_ITEM = "http://item\\.jd\\.com/(\\d+)\\.html";
    private static final String URL_SEARCH = ".*search.jd.com.*";
    private static final String URL_LIST_FORMAT_JDSELF = "http://list.jd.com/list.html?cat=%s&delivery=1&delivery_daofu=3";
    private static final String URL_LIST_FORMAT_NORMAL = "http://list.jd.com/list.html?cat=%s&delivery=0&delivery_daofu=0";
    private static final String URL_AJAX_PRICE = "http://p.3.cn/prices/mgets?skuIds=J_%s&type=1";
    private static final String URL_AJAX_PRICE_FORMAT = ".*p\\.3\\.cn.*";
    public static final String URL_PRICE_BOX_FORMAT = "http://www.boxz.com/products/360buy-%s.shtml";
    public static final String URL_PRICE_BOX = "http://www.boxz.com/products/360buy-\\d+.shtml";

    protected static Logger logger = LoggerFactory.getLogger(JDSpider.class);

    public static AtomicInteger pageNum = new AtomicInteger(0);

    public JDSpider()
    {
        getSite().setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.65 Safari/537.31");
        try
        {
            addURLRoute(URL_ITEM, "JDItemPageProcess");
            addURLRoute(URL_PRICE_BOX, "PriceBoxPageProcess");
            addURLRoute(URL_LIST, "JDListPageProcess");
            addURLRoute(URL_AJAX_PRICE_FORMAT, "JDAJAXPageProcess");
        }
        catch (NoSuchMethodException e)
        {
            e.printStackTrace();
        }
    }


    private void JDListPageProcess(Page page)
    {
        pageNum.incrementAndGet();
        page.addTargetRequests(
                page.getHtml().xpath("//ul[@class=\"gl-warp clearfix\"]").links().regex(URL_ITEM, 0).all());
        page.addTargetRequests(
                page.getHtml().xpath("//div[@id=\"J_bottomPage\"]/span/a").links().regex(URL_LIST).all());
        page.setSkip(true);
    }

    private void JDItemPageProcess(Page page)
    {
        pageNum.incrementAndGet();
        String id = page.getUrl().regex(URL_ITEM, 1).toString();
        //page.putField("name", page.getHtml().xpath("//ul[@id=\"parameter2\"]/li/@title").toString());
        page.putField("idInMarket", id);
        //page.putField("description", page.getHtml().xpath("//div[@id=\"name\"]/h1/text()").toString());
        page.putField("name", page.getHtml().xpath("//div[@id=\"name\"]/h1/text()").toString());
        page.putField("url", page.getUrl().toString());
        page.putField("market", "京东");
        page.putField("pics", page.getHtml().xpath("//div[@id=\"spec-list\"]/div/ul/li/img/@src").all());
        Request request = new Request(String.format(URL_AJAX_PRICE, id));
        request.putExtra("id", id);
        Map<String, String> dic = new HashMap<String, String>();
        page.putField("stock", dic);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        String dateStr = sdf.format(date);
        page.putField("updateTime", dateStr);
        request.putExtra("time", dateStr);
        page.addTargetRequest(request);
    }

    private void JDAJAXPageProcess(Page page)
    {
        try
        {
            pageNum.incrementAndGet();
            String jsonstr = page.getRawText();
            jsonstr = jsonstr.substring(jsonstr.indexOf("{"), jsonstr.indexOf("]"));
            JSONObject jsonObject = JSON.parseObject(jsonstr);
            page.putField("idInMarket", page.getRequest().getExtra("id"));
            page.putField("updateTime", page.getRequest().getExtra("time"));
            String price = jsonObject.get("p").toString();
            page.putField("price", Integer.parseInt(price.substring(0, price.length() - 3)));
        }
        catch (Exception e)
        {
            logger.error("get price error", e);
            e.printStackTrace();
        }
    }

    private void PriceBoxPageProcess(Page page)
    {
        pageNum.incrementAndGet();
        //从Html中取出时间和价格列表
        Selectable priceStr = page.getHtml().xpath("//input[@id=chart_data]/@value");
        List<String> dateList = priceStr.regex("\\{x:(\\d+),y:(\\d+\\.\\d+)\\}").all();
        List<String> priceList = priceStr.regex("\\{x:(\\d+),y:(\\d+\\.\\d+)\\}", 2).all();
        page.putField("source_id", page.getRequest().getExtra("id"));
        //page.putField("description", page.getRequest().getExtra("description"));
        //将两个列表保存到一个map中
        int size = dateList.size();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Map<String, String> priceMap = new HashMap<String, String>(size);
        for(int index = 0; index < size; index++)
        {
            String dateRawStr = dateList.get(index);
            String dateStr = formatter.format(new Date(Long.parseLong(dateRawStr)));
            priceMap.put(dateStr, priceList.get(index));
        }
        page.putField("price_list", priceMap);
    }


    public static void main(String[] args)
    {
        logger.info("================spider start!========================");
        MongoClient client = new MongoClient("192.168.1.250");
        Spider spider = Spider.create(new JDSpider())
                .addPipeline(new MongoPipeline(client).setDefaultMongoCollection("commodity"))
                .thread(5);
        /*
        List<String> urlList = GetUrlFromFile(args[0]);

        for (String url : urlList)
        {
            spider.addUrl(url);
        }*/
        spider.addUrl("http://item.jd.com/1594285253.html");
        spider.run();
        logger.info("====================get {} pages===========================", pageNum);
    }

    private static List<String> GetUrlFromFile(String fileName)
    {
        List<String> urlList = new ArrayList<String>(20);
        File file = new File(fileName);
        BufferedReader reader = null;
        try
        {
            reader = new BufferedReader(new FileReader(file));
            String tempString;
            while ((tempString = reader.readLine()) != null)
            {
                String[] tmp_list = tempString.split(" ");
                if(tmp_list.length == 1 || tmp_list[1].equals("1"))
                    urlList.add(String.format(URL_LIST_FORMAT_JDSELF, tempString));
                else
                    urlList.add(String.format(URL_LIST_FORMAT_NORMAL, tempString));
            }
        }
        catch (IOException e)
        {
            logger.error("error to get url list from csv file");
        }
        finally
        {
            if (reader != null)
            {
                try
                {
                    reader.close();
                }
                catch (IOException e)
                {
                    logger.error("close file error");
                }

            }
        }
        return urlList;
    }
}

