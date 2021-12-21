package com.Jackson.test

import org.apache.spark.sql.sources.In


/** movies.csv
  *
  * 1^                  mid  电影
  * Toy Story (1995)^   name 电影名字
  * ^                   descri(空的) 电影描述
  * 81 minutes^         timelong 电影时长
  * March 20, 2001^     issue 发行日期
  * 1995^               shoot 拍摄日期
  * English ^           language 电影语言
  * Adventure|Fantasy ^ genres 电影类别
  * Tom Hankscek ney|Wallace Shawn ^ actors 电影演员
  * John Lasseter       directors 电影导演
  * */

//这里需要说明，issue 发行日期不是标准的日期格式（都统一用String类型）
// 如果在其他地方需要他是日期格式，可以在转换成Int类型
//一般在case class里面不日期不用Int类型

case class MovieTest(val mid: Int, val name: String, val descri: String,
                     val timelong: String, val issue: String, val shoot: String,
                     val language: String, val genres: String, val actors: String,
                     val directors: String)

/**
  * 1,31,2.5,1260759144           ratings.csv
  *
  * 1,                           uid 用户id
  * 31,                          mid 电影id
  * 2.5,                         score 电影评分
  * 1260759144                   timstamp 电影时间戳
  *
  */

case class RatingTest(val uid: Int, val mid: Int,
                      val score: Double, val timstamp: Int)


/**
  * 15,1955,dentist,1193435061  tags.csv
  *
  * 5,                    uid       用户id
  * 1955,                 mid       电影id
  * dentist,              tag       电影标签
  * 1193435061            timestamp 电影时间戳
  *
  */
case class TagTest(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)

/**
  * mongoDB配置对象
  *
  * @param uri
  * @param db
  */

case class MongoConfig(val uri: String, val db: String)

/**
  * ES配置对象
  */
case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)






