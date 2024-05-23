package org.sunbird.core.util

import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase}

import java.util
import scala.collection.JavaConverters._
import scala.concurrent.duration.Duration
import scala.concurrent.{Await}

class MongoUtil (host: String, port: Int, database: String) {

  val mongoClient: MongoClient = MongoClient(s"mongodb://$host:$port")
  val mongoDatabase: MongoDatabase = mongoClient.getDatabase(database)

  def aggregate(collection: String, pipeline: List[Bson]): util.List[Document] = {
    val mongoCollection: MongoCollection[Document] = mongoDatabase.getCollection(collection)
    val result = mongoCollection.aggregate(pipeline).toFuture()
    Await.result(result, Duration.Inf).asJava
  }

  def insertMany(collection: String, documents: Seq[Document]): Boolean = {
    val mongoCollection: MongoCollection[Document] = mongoDatabase.getCollection(collection)
    val result = mongoCollection.insertMany(documents).toFuture()
    Await.result(result, Duration.Inf).wasAcknowledged()
  }

}
