package com.yjf.applications.recommendation

import com.yjf.utils.Similarity
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * @ClassName itemCF
  * @Description 基于物品的推荐算法itemCF,输入user，item，rating分别对应用户，商品，得分
  * @Author HuZhongJin
  * @Date 2018/10/8 12:55
  * @Version 1.0
  */

/**
  * itemCFModel的参数
  */
private[recommendation] trait itemCFModelParams extends Params {

  /**@group Params*/
  val userColumn = new Param[String](this, "userColumn", "用户列名")

  /**@group getParams*/
  val getUserColumn: String = $(userColumn)

  /**@group Params*/
  val itemColumn = new Param[String](this, "itemColumn", "商品列名")

  /**@group getParams*/
  val getItemColumn: String = $(itemColumn)

  /**@group Params*/
  val ratingColumn = new Param[String](this, "ratingColumn", "得分列")

  /**@group getParams*/
  val getRatingColumn: String = $(ratingColumn)

  /**@group Params*/
  val n = new IntParam(this, "n", "推荐商品的个数", ParamValidators.gtEq(1))

  /**@group getParam*/
  val getN: Int = $(n)
}

/**
  * itemCF的参数
  */
private[recommendation] trait itemCFParams extends itemCFModelParams with Params {

  /**@group Params*/
  val k = new IntParam(this, "k", "最近邻的个数", ParamValidators.gtEq(1))

  /**@group getParam*/
  val getK: Int = $(k)

  /**set default params*/
  setDefault(
    k -> 10,
    n -> 5,
    userColumn -> "user",
    itemColumn -> "item",
    ratingColumn -> "rating"
  )
}

class itemCF(override val uid: String, similarity: String) extends itemCFParams with DefaultParamsWritable{

  override def copy(extra: ParamMap): itemCF = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("itemCF"))

  /**@group setParams*/
  def setK(value: Int): this.type = set(k, value)

  /**@group setParams*/
  def setN(value: Int): this.type = set(n, value)

  /**@group setParams*/
  def setUserColumn(value: String): this.type = set(userColumn, value)

  /**@group setParams*/
  def setItemColumn(value: String): this.type = set(itemColumn, value)

  /**@group setParams*/
  def setRatingColumn(value: String): this.type = set(ratingColumn, value)

  /**@group fit*/
  def fit(spark: SparkSession, dataSet: DataFrame): itemCFModel = {

    import spark.implicits._

    //每个商品的评分次数
    val numRatingPerItem = dataSet.groupBy($(userColumn)).count().alias("nor")

    //在原记录的基础上添加item的打分者的数量
    val ratingsWithSize = dataSet.join(numRatingPerItem, $(itemColumn))

    val joined = ratingsWithSize.join(ratingsWithSize, "user")
      .toDF("user", "item1", "rating1", "nor1", "item2", "rating2", "nor2")

    //计算稀疏矩阵

    val sparseMatrix = joined.groupBy($"item1", $"item2").agg(count($"user").alias("userForAll"), first($"nor1").alias("userForA"), first($"nor2").alias("userForB")).cache()

    //计算物品相似度
    //需要把相似度矩阵实例化到db或者hdfs上
    val similar = sparseMatrix.rdd.map(row => {
      val userForAll = row.getLong(2)
      val userFor1 = row.getLong(3)
      val userFor2 = row.getLong(4)

      val cooccurrence = Similarity.CoOccurrence(userForAll, userFor1)
      val improvecooc = Similarity.ImprovedCoOccurrence(userForAll, userFor1, userFor2)
      (row.getString(0), row.getString(1), cooccurrence, improvecooc)
    }).toDF("item1", "item2", "cooc", "improvecooc")
    copyValues(new itemCFModel(uid, dataSet, similar))
  }
}

class itemCFModel private[applications] (
                                          override val uid: String,
                                          @transient user_item: DataFrame,
                                          @transient similar: DataFrame) extends itemCFModelParams {

  /**@group setParams*/
  def setN(value: Int): this.type = set(n, value)

  /**@group setParams*/
  def setUserColumn(value: String): this.type = set(userColumn, value)

  /**@group setParams*/
  def setItemColumn(value: String): this.type = set(itemColumn, value)

  /**@group setParams*/
  def setRatingColumn(value: String): this.type = set(ratingColumn, value)

  /**
    * 为所有的用户推荐商品
    * @param user 用户表
    * @param similarityMeasure 计算相似度的方法
    * @return
    */
  def recommendForAllUsers(spark: SparkSession, user: DataFrame, similarityMeasure: String): DataFrame = {

    import spark.implicits._

    val sim = similar.select("item1", "item2", similarityMeasure)
    //进行左连接
    val user_item_joined = user.join(user_item, user("user") === user_item("user"), "left")

    //获取用户感兴趣的物品和其他物品的相似度
    val user_sim = user_item_joined.join(sim, user_item_joined("item") === sim("item1"))
        .selectExpr("user",
          "item1",
          "item2",
          similarityMeasure,
          s"$similarityMeasure * rating as sim"
        )

    val df = user_sim.groupBy(user_sim("user"), user_sim("item2")).agg(sum(user_sim(similarityMeasure)).alias("score"))

    df.orderBy(df("rating").desc)
        .rdd
        .map(r => (r.getString(0), (r.getString(1), r.getString(2))))
        .groupByKey()
        .mapValues(r => {
          var sequence = Seq[(String, Double)]()
          val iter = r.iterator
          var count = 0
          while (iter.hasNext && count < $(n)) {
            val rat = iter.next()
            if (rat._2 != Double.NaN)
              sequence :+= (rat._1, rat._2)
            count += 1
          }
          sequence
        }).toDF("user", "recommended")
  }

  override def copy(extra: ParamMap): itemCFModel = {
    val copied = new itemCFModel(uid, user_item, similar)
    copyValues(copied, extra)
  }
}
