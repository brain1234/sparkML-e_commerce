package com.yjf.applications.example

import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.{MinMaxScaler, PCA, Tokenizer, Word2Vec}
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * @ClassName test
  * @Description 示例代码
  * @Author HuZhongJin
  * @Date 2018/9/29 0:14
  * @Version 1.0
  */

private[example] trait testParam extends Params{

  /**参数k的说明*/
  /**@group Params*/
  val k = new IntParam(this, "k", "对k的说明", ParamValidators.gtEq(0))

  setDefault(
    k -> 2
  )

  /**@group getParams*/
  val getK: Int = $(k)
}

class test(override val uid: String) extends testParam with DefaultParamsWritable{

  def this() = this(Identifiable.randomUID("test"))

  /**@group setParams*/
  def setK(value: Int): this.type = set(k, value)

  /**@group fit*/
  /**如果输出是需要训练的模型，使用这个方法*/
  def fit(dataset: Dataset[_]): testModel = {
    copyValues(new testModel(uid).setParent(this))
  }

  /**@group transform*/
  /**如果模型不需要训练，则使用这个方法*/
  def transform(dataset: Dataset[_]): DataFrame = {
    dataset.toDF()
  }

  override def copy(extra: ParamMap): test = defaultCopy(extra)
}



case class Coltest(col1:String,col2:Int) extends Serializable //定义字段名和类型
object test extends DefaultParamsReadable[test]{

  override def load(path: String): test = super.load(path)

  def main(args: Array[String]): Unit = {
    val lir = new LinearRegression()
    val scaler = new MinMaxScaler()
    val als = new ALS()
    val s = new test().setK(1)
    val r = Seq(("a",1),("b",2),("c",3))
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(r)
    val test: Dataset[Coltest]=rdd.map{line=>
      Coltest(line._1,line._2)
    }.toDS
    val df = s.transform(test)
    df.show(1)
  }
}

class testModel private[applications] (override val uid: String) {
  
}
