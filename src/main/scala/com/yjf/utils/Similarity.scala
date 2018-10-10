package com.yjf.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @ClassName Similarity
  * @Description 相似度计算的公共类
  * @Author HuZhongJin
  * @Date 2018/10/9 17:39
  * @Version 1.0
  */

object Similarity {

  /**
    * 同现相似度
    * |N(i) ∩ N(j)| / |N(i)
    * @param UserItemRatingMatrix 用户-物品-评分 矩阵
    */
  def CoOccurrence(userForAll: Long, userFor1: Long): Double= {
    userForAll / userFor1
  }

  /**
    * 改进的同现相似度
    * |N(i) ∩ N(j)| / sqrt(|N(i)||N(j)|)
    */
  def ImprovedCoOccurrence(userForAll: Long, userFor1: Long, userFor2: Long) = {
    userForAll / math.sqrt(userFor1 * userFor2)
  }

  /**
    * 欧几里得距离相似度
    */
  def Eucledian(va: String) = {

  }

  /**
    * 皮尔逊相似度
    */
  def Pearson(va: String) = {

  }

  /**
    * 余弦相似度
    */
  def Cosine(va: String) = {

  }

  /**
    * 改进的余弦相似度
    */
  def ImprovedCosine(va: String) = {

  }

  /**
    * Jaccard 系数
    */
  def Jaccard(va: String) = {

  }
}
