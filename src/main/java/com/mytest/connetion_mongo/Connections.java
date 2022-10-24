package com.mytest.connetion_mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by ua07 on 11/19/19.
 */
public class Connections {

    public static void insert(MongoCollection<Document> collection){
        Document document = new Document("name", "dog");
        List<Document> documents = new ArrayList<Document>();
        documents.add(document);
        collection.insertMany(documents);
    }


    public static void main(String[] arg) throws Exception {
        MongoClient mongoClient = null;
        try {
            mongoClient = new MongoClient("localhost",27018);

            MongoDatabase db = mongoClient.getDatabase("MonLab");
            System.out.println("connetion database OK");
            System.out.println("MongoDatabase info is :"+db.getName());

            MongoCollection<Document> collection = db.getCollection("Movie3");
            System.out.println("get collection OK");



        }catch(Exception e){
            System.out.println(e.getClass().getName()+" : "+e.getMessage());
        }
        finally{
            mongoClient.close();
        }



    }
}
