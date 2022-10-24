package com.mytest.connetion_mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;


/**
 * Created by ua07 on 11/19/19.
 */
public class Connect_Mongo {

    /**
     *
     * @param db ---database
     */
    public static void insert(MongoDatabase db){
        MongoCollection<Document> collection = db.getCollection("Movie3");
        Document document = new Document();
        document.append("name","nihao").append("score",80);
        collection.insertOne(document); //

        System.out.println("inert one is OK");

        Document document1 = new Document();
        Document document2 = new Document();
        Document document3 = new Document();
        Document document4 = new Document();

        document1.append("name","nihao1").append("score",81);
        document2.append("name","nihao11").append("score",90);

        document3.append("name","nihao12").append("score",90);
        document4.append("name","nihao2").append("score",82);

//        List<Document> documents = new ArrayList<Document>();
        List<Document> documents = new LinkedList<Document>();
        documents.add(document1);
        documents.add(document2);
        documents.add(document3);
        documents.add(document4);

        collection.insertMany(documents);
        System.out.println("inert many is OK");

    }

    /**
     *
     * @param collection
     */
    public static void delete(MongoCollection collection){
        collection.deleteOne(new Document("name","nihao")); // delete fiirst name is nihao
//        collection.deleteOne(new Document("score",90));  //delete first score is 90
        collection.deleteMany(new Document("score",90));
        System.out.println("delete OK");
    }

    public static void update(MongoCollection collection){
        collection.updateOne(eq("score",81),new Document("$set",new Document("score",91)));
        System.out.println("update is OK");
    }

    public static void search(MongoDatabase db){
        MongoCollection<Document> collection1 = db.getCollection("Movie3");
        MongoCursor<Document> cursor = collection1.find().iterator();
        try{
            while(cursor.hasNext()){
                System.out.println(cursor.next().toJson());
            }
        }finally{
            cursor.close();
        }

    }

    public static void main(String[] arg) throws Exception {
        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient("localhost",27030);

            MongoDatabase db = mongoClient.getDatabase("MonLab");
            System.out.println("connetion database OK");
            System.out.println("MongoDatabase info is :"+db.getName());

            MongoCollection collection = db.getCollection("Movie3");

            System.out.println("get collection OK");

            // inert
//            Connect_Mongo.insert(db);

            //delete
//            delete(collection);

            // update
//            update(collection);

            // serach

            Connect_Mongo.search(db);

        }catch(Exception e){
            System.out.println(e.getClass().getName()+" : "+e.getMessage());
        }
        finally{
            mongoClient.close();
        }



    }
}
