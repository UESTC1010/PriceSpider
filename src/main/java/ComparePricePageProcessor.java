import com.alibaba.fastjson.JSON;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Request;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.pipeline.Pipeline;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.scheduler.RedisScheduler;
import us.codecraft.webmagic.selector.Selectable;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class ComparePricePageProcessor implements PageProcessor
{
    public static final String URL_CONTENT = "http://www.boxz.com/products/360buy-%s.shtml";
    private Site site = Site.me()
            //.setDomain("list.jd.com")
            .setUserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_2) AppleWebKit/537.31 (KHTML, like Gecko) Chrome/26.0.1410.65 Safari/537.31");
    public void process(Page page)
    {
        //从Html中取出时间和价格列表
        Selectable priceStr = page.getHtml().xpath("//input[@id=chart_data]/@value");
        List<String> dateList = priceStr.regex("\\{x:(\\d+),y:(\\d+\\.\\d+)\\}").all();
        List<String> priceList = priceStr.regex("\\{x:(\\d+),y:(\\d+\\.\\d+)\\}", 2).all();
        page.putField("id", page.getRequest().getExtra("id"));
        page.putField("description", page.getRequest().getExtra("description"));
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
        page.putField("priceMap", priceMap);

    }

    public Site getSite()
    {
        return site;
    }


    public static void main(String[] args)
    {
        Spider spider = Spider.create(new ComparePricePageProcessor());
        File urldir = new File("/Users/apple/list.jd.com");
        //File urldir = new File(args[0]);
        File[] files = urldir.listFiles();
        if(files != null)
        {
            for (File file : files)
            {
                try
                {
                    //从本地读取之前抓取的JD商品ID
                    FileReader fileReader = new FileReader(file.getAbsolutePath());
                    StringBuilder sb = new StringBuilder();
                    BufferedReader br = new BufferedReader(fileReader);
                    String s = br.readLine();
                    while(s != null)
                    {
                        sb.append(s);
                        s = br.readLine();
                    }
                    br.close();
                    Commodity commodity = JSON.parseObject(sb.toString(), Commodity.class);
                    String url = String.format(URL_CONTENT, commodity.id);
                    //将url添加到spider中
                    Request request = new Request(url);
                    request.putExtra("description", commodity.description);
                    request.putExtra("id", commodity.id);
                    spider.addRequest(request);
                }
                catch (FileNotFoundException e)
                {
                    System.out.println(e.toString());
                }
                catch (IOException e)
                {
                    System.out.println(e.toString());
                }
            }
        }

        MongoClient client = new MongoClient("192.168.1.250");
        List<Pipeline> pipelines = new ArrayList<Pipeline>();
        pipelines.add(new MongoPipeline(client).setDefaultMongoCollection("JDCollection"));
        spider.setPipelines(pipelines)
                .thread(5).run();
    }
}
