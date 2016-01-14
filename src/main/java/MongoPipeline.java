import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.font.TrueTypeFont;
import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.DBPipeline;
import us.codecraft.webmagic.pipeline.Pipeline;


public class MongoPipeline extends DBPipeline<MongoClient> implements Pipeline
{
    private Logger logger = LoggerFactory.getLogger(getClass());
    protected MongoDatabase defaultMongoDatabase = DBConnector.getDatabase("test");
    protected MongoCollection<Document> defaultMongoCollection;

    public MongoPipeline(MongoClient mongoConnector)
    {
        super(mongoConnector);
    }

    public void process(ResultItems resultItems, Task task)
    {
        if(this.defaultMongoCollection != null)
        {
            if(resultItems.get("name") != null)
            {
                //Document document = new Document(resultItems.getAll());
                try
                {
                    Document document = new Document(resultItems.getAll());
                    this.defaultMongoCollection.updateOne(
                            new Document("updateTime", resultItems.get("updateTime")), new Document(
                                    "$set", document), new UpdateOptions().upsert(true));
                }
                catch (Exception e)
                {
                    logger.warn("save data in mongo error", e);
                }
            }
            else
            {
                try
                {
                    Document search = new Document();
                    search.put("updateTime", resultItems.get("updateTime"));
                    search.put("idInMarket", resultItems.get("idInMarket"));
                    this.defaultMongoCollection.updateOne(
                            search,
                            new Document("$set", new Document("price",
                                    resultItems.get("price"))));
                }
                catch (Exception e)
                {
                    logger.warn("save data in mongo error", e);
                }
            }
        }
    }

    public MongoPipeline setDefaultMongoDatabase(String database)
    {
        this.defaultMongoDatabase = DBConnector.getDatabase(database);
        return this;
    }

    public MongoPipeline setDefaultMongoCollection(String collectionName)
    {
        this.defaultMongoCollection = defaultMongoDatabase.getCollection(collectionName);
        return this;
    }
}
