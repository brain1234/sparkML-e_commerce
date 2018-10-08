package com.yjf.applications.recommendation

import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators, Params}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}

/**
  * @ClassName itemCF
  * @Description 基于物品的推荐算法itemCF
  * @Author HuZhongJin
  * @Date 2018/10/8 12:55
  * @Version 1.0
  */

/**
  * itemCF的参数
  */
private[recommendation] trait itemCFParams extends Params {

  /**@group Params*/
  val k = new IntParam(this, "k", "最近邻的个数", ParamValidators.gtEq(1))

  /**@group getParam*/
  val getK: Int = $(k)

  /**@group Params*/
  val n = new IntParam(this, "n", "推荐商品的个数", ParamValidators.gtEq(1))

  /**@group getParam*/
  val getN: Int = $(n)

  /**set default params*/
  setDefault(
    k -> 5,
    n -> 3
  )
}

class itemCF(override val uid: String) extends itemCFParams with DefaultParamsWritable{

  override def copy(extra: ParamMap): itemCF = defaultCopy(extra)

  def this() = this(Identifiable.randomUID("itemCF"))

  /**@group setParams*/
  def setK(value: Int): this.type = set(k, value)

  /**@group setParams*/
  def setN(value: Int): this.type = set(n, value)

  /**@group fit*/
  def fit(dataSet: DataFrame): itemCFModel = {

  }


}

class itemCFModel private[applications] (
                                          override val uid: String,
                                          val s: String) extends itemCFParams {
  override def copy(extra: ParamMap): itemCFModel = {
    val copied = new itemCFModel(uid, s)
    copyValues(copied, extra)
  }
}
