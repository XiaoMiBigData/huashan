package com.Jackson.test


import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by root on 2020
  */

object DataLoaderTest {
  //MongoDB 中的表 Collection
  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"

  // Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  def main(args: Array[String]): Unit = {

    val DATAFILE_MOVIES = "D:\\flie\\MoviesData\\movies.csv"
    val DATAFILE_RATINGS = "D:\\flie\\MoviesData\\ratings.csv"
    val DATAFILE_TAGS = "D:\\flie\\MoviesData\\tags.csv"

    //这里封装了一个可变集合Map[String, Any]。String是key，Any是value
    val params1 = scala.collection.mutable.Map[String, Any]()
    params1 += "spark.core" -> "local[2]"
    params1 += "spark.name" -> "DataLoaderTest"
    params1 += "mongo.uri" -> "mongodb://192.168.1.126:27017/recom"
    params1 += "mongo.db" -> "recom"


    //定义MongoDB配置对象
    implicit val mongoConf = new MongoConfig(params1("mongo.uri").asInstanceOf[String],
      params1("mongo.db").asInstanceOf[String])


    //声明spark环境
    val conf: SparkConf = new SparkConf()
      .setAppName(params1("spark.name").asInstanceOf[String])
      .setMaster(params1("spark.core").asInstanceOf[String])

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    //获取sc,目的设置log打印日志级别为"ERROR"
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")

    //加载数据集
    val movieRDD: RDD[String] = spark.sparkContext.textFile(DATAFILE_MOVIES)

    val ratingRDD: RDD[String] = spark.sparkContext.textFile(DATAFILE_RATINGS)

    val tagRDD: RDD[String] = spark.sparkContext.textFile(DATAFILE_TAGS)

    import spark.implicits._
    //将RDD+schema转换成DataFrame

    //movieDF的DataFrame
    val movieDF: DataFrame = movieRDD.map(line => {
      //由于jvm，需要一个转义符"\\"
      val x: Array[String] = line.split("\\^")

      //trim使我们的数据紧凑并且不会出现空格错误
      //x里面的数据都是String,因此x(0)还需要转化成为Int
      MovieTest(x(0).trim.toInt, x(1).trim, x(2).trim,
        x(3).trim, x(4).trim, x(5).trim, x(6).trim,
        x(7).trim, x(8).trim, x(9))
    }).toDF()


    //ratingDF的DataFrame
    val ratingDF = ratingRDD.map(line => {
      val x: Array[String] = line.split(",")
      //x里面的数据都是String,因此x(0)还需要转化成为Int
      RatingTest(x(0).trim.toInt, x(1).trim.toInt,
        x(2).trim.toDouble, x(3).trim.toInt)
    }).toDF()

    // ratingDF.show()//默认显示20行


    //tagDF的DataFrame
    val tagDF: DataFrame = tagRDD.map(line => {
      val x: Array[String] = line.split(",")
      TagTest(x(0).trim.toInt, x(1).trim.toInt, x(2).trim, x(3).trim.toInt)
    }).toDF()

    //tagDF.show()


    //将数据写入MongDB
    //先写入到mongdb（此时下面是没注销的，但是为了整体代码，执行完就注销了）
    //storeDataInMongo(movieDF, ratingDF, tagDF)

    //mongodb写完，这行就注销了，然后写入es(一定要注意)
    //storeDataInMongo(movieDF, ratingDF, tagDF)

    //将数据保存到ES
    //不是将原始数据保存到ES,而是将Movie表和stag表join并且聚合
    //因为ES一般保存文档的，索引Movie表和tag表里面都要文字描述

    //引入内置函数库
    import org.apache.spark.sql.functions._
    //将数据保存到ES,做数据缓存，便于计算更加快速
    movieDF.cache()
    tagDF.cache()

    //先分组后聚合
    //concat_ws，分割符
    //collect_set，指定哪一列
    //表名叫tags
    val tagCollectDF: DataFrame = tagDF.groupBy("mid")
      .agg(concat_ws("|", collect_set("tag")).as("tags"))


    //进行关联
    //join方式left
    //条件是Seq，俩张表"mid",是否相同
    //select进行进一步过滤
    val esMovieDF: DataFrame = movieDF.join(tagCollectDF,
      Seq("mid", "mid"), "left")
      .select("mid", "name", "descri", "timelong",
        "issue", "shoot", "language", "genres", "actors", "directors", "tags")

    esMovieDF.show()

    spark.stop()
    sc.stop()

    //这些是显示参数
    def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {

      //创建到MongoDB的连接
      val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

      //删除mongodb对应的表
      mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()
      mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()
      mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()



      //写入MongoDB
      movieDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", MOVIES_COLLECTION_NAME)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

      ratingDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", RATINGS_COLLECTION_NAME)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

      tagDF.write
        .option("uri", mongoConfig.uri)
        .option("collection", TAGS_COLLECTION_NAME)
        .mode("overwrite")
        .format("com.mongodb.spark.sql")
        .save()

      //创建mongodb索引
      mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
      mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
      mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))

      mongoClient.close()
    }
  }
}
