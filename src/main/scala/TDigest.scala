package com.schwartzmx.tdigest

import scala.collection.mutable.TreeMap
import scala.util.Random
import util.control.Breaks._


object DoubleOrdering extends Ordering[Double] {
  def compare(key1:Double, key2:Double) = key1.compareTo(key2)
}

class TDigest (var delta: Double = 0.01, var K: Int = 25) {
  var centroids: TreeMap[Double,Centroid] = TreeMap.empty(DoubleOrdering)
  var totalCount: Double = 0

  def ++(otherDigest:TDigest): TDigest = {
    val data = (this.centroids.values ++ otherDigest.centroids.values).toSeq
    val newDigest = new TDigest(this.delta, this.K)

    if (data.length > 0) {
      val rl: Seq[Centroid] = Random.shuffle(data)
      rl.foreach(c => newDigest.update(c.mean, c.count))
    }
    newDigest
  }

  def len(): Int = this.centroids.size

  override def toString() : String = s"<T-Digest: totalCount=${this.totalCount}, centroids=${this.len()}>"

  def addCentroid(c: Centroid): Unit = {
    if (!this.centroids.contains(c.mean)) {
      this.centroids.put(c.mean, c)
    }
    else {
      (this.centroids.get(c.mean): @unchecked) match {
        case Some(x:Centroid) => x.update(c.mean, c.count)
      }
    }
  }

  def computeCentroidQuantile(c: Centroid): Double = {
    val d = this.totalCount
    val valuesSlice = this.centroids.range(Double.NegativeInfinity, c.mean)
    var cumulativeSum: Double = 0.0
    valuesSlice.values.foreach(cumulativeSum += _.count)
    (c.count / 2.0 + cumulativeSum) / d
  }

  def updateCentroid(c: Centroid, x: Double, w: Double): Unit = {
    this.centroids.remove(c.mean)
    c.update(x, w)
    this.addCentroid(c)
  }

  def findCeilingKey(x: Double): Option[Double] = {
    val from = this.centroids.from(x)
    if(from.isEmpty) None
    else Some(from.firstKey)
  }

  def findFloorKey(x: Double): Option[Double] = {
    val to = this.centroids.to(x)
    if (to.isEmpty) None
    else Some(to.lastKey)
  }

  def findClosestCentroids(x: Double): Seq[Centroid] = {
    val default = Double.NegativeInfinity
    val ceilKey = findCeilingKey(x) match {
      case Some(value) => value
      case None => default
    }
    val floorKey = findFloorKey(x) match {
      case Some(value) => value
      case None => default
    }

    if (ceilKey == default) Seq(this.centroids.get(floorKey).get)
    else if (floorKey == default) Seq(this.centroids.get(ceilKey).get)
    else {
      if (Math.abs(floorKey - x) < Math.abs(ceilKey - x))
        Seq(this.centroids.get(floorKey).get)
      else if (Math.abs(floorKey -x) == Math.abs(ceilKey - x) && (ceilKey != floorKey))
        Seq(this.centroids.get(ceilKey).get, this.centroids.get(floorKey).get)
      else
        Seq(this.centroids.get(ceilKey).get)
    }
  }

  def threshold(q: Double): Double = 4 * this.totalCount * this.delta * q * (1 - q)

  def minVal(x: Double, y: Double): Double = {
    if (x <= y) x
    else y
  }

  def maxVal(x:Double, y:Double): Double = {
    if (x >= y) x
    else y
  }

  def remove(i: Int, s: Seq[Centroid]): Seq[Centroid] = s diff Seq(s(i))

  def update(x:Double, w:Double=1): Unit = {
    this.totalCount += w
    var we = w

    if(len() == 0) {
      addCentroid(new Centroid(x, we))
      return
    }

    var S = this.findClosestCentroids(x)
    while (S.size != 0 && we > 0) { 
      breakable {
        val j = Random.nextInt(S.size)
        val cJ = S(j)
        val q = computeCentroidQuantile(cJ)

        if (cJ.count + we > threshold(q)) {
          S = remove(j, S)
          break
        }

        val deltaW = minVal(threshold(q) - cJ.count, we)
        updateCentroid(cJ, x, deltaW)
        we -= deltaW
        S = remove(j, S)
      }
    }

    if (we > 0) 
      addCentroid(new Centroid(x, we))

    if (len() > this.K / this.delta) 
      compress()
  }

  def batchUpdate(values: Seq[Double], w: Double=1): Unit = {
    values.foreach(update(_, w))
    compress()
  }

  def compress(): Unit = {
    val T = new TDigest(this.delta, this.K)
    val centroids = this.centroids.values.toSeq
    val rl:Seq[Centroid] = Random.shuffle(centroids)
    this.centroids.clear
    rl.foreach(c => T.update(c.mean, c.count))
    this.centroids = T.centroids
  }

  def successorKey(k: Double): Double = {
    // get an iterator from k, take the 2 elements (inclusive of k) and get the last
    this.centroids.keysIteratorFrom(k).take(2).toSeq.lastOption.getOrElse(k)
  }

  def previousKey(k: Double): Double = {
    // there is not an efficient way (AFAIK) to do this currently in the scala TreeMap implementation
    // see https://issues.scala-lang.org/browse/SI-8621
    this.centroids.iterator.takeWhile(_._1 < k).toSeq.lastOption.getOrElse((k,None))._1
  }

  def percentile(p:Double): Double = {
    if (!(0 <= p && p <= 100.0))
      throw new IllegalArgumentException("Invalid value passed,  p must be between 0 and 100, inclusive.")

    var t: Double = 0
    var pe = p / 100.0 * this.totalCount
    this.centroids.keys.zipWithIndex.foreach { case (key, i) =>
      val cI = this.centroids.get(key)
      val k = cI.get.count
      if (pe < t + k) {
        if (i == 0) return cI.get.mean
        else if (i == len() - 1) return cI.get.mean
        else {
          val sKey = successorKey(key)
          val pKey = previousKey(key)
          val delta = (this.centroids.get(sKey).get.mean - this.centroids.get(pKey).get.mean) / 2.0
          return cI.get.mean + ((pe - t) / k - 0.5) * delta
        }
      }
      t += k
    }
    this.centroids.max._2.mean
  }

  def quantile(q: Double): Double = {
    var t: Double = 0
    val N = this.totalCount

    if (len() == 1) {
      if (q >= this.centroids.min._1) return 1
      else return 0
    }
    
    this.centroids.keys.zipWithIndex.foreach { case (key, i) =>
      val cI = this.centroids.get(key)
      val delta: Double = 
        if (i == len() - 1)
          (cI.get.mean - this.centroids.get(previousKey(key)).get.mean) / 2.0
        else
          (this.centroids.get(successorKey(key)).get.mean - cI.get.mean) / 2.0

      val z = maxVal(-1, (q - cI.get.mean) / delta)
      if (z < 1) return t / N + cI.get.count / N * (z + 1) / 2

      t += cI.get.count
    }
    1.0
  }

  def trimmedMean(p1: Double, p2: Double): Double = {
    if ((p1 > p2) || !(p1 >= 0 && p1 <= 100) || !(p2 >= 0 && p2 <= 100))
      throw new IllegalArgumentException("p1 must be between 0 and 100 and less than p2.")
    
    var s, k, t: Double = 0
    val pe1 = p1 / 100.0 * this.totalCount 
    val pe2 = p2 / 100.0 * this.totalCount

    breakable {
      this.centroids.keys.zipWithIndex.foreach { case (key, i) =>
        val cI = this.centroids.get(key)
        val kI = cI.get.count
        var nU = 1.0
        if (pe1 < t + kI) {
          if (t < pe1) nU = interpolate(i, key, pe1-t)

          s += nU * kI * cI.get.mean
          k += nU * kI
        }

        if (pe2 < t + kI) {
          nU = interpolate(i, key, pe2-t)
          s -= nU * kI * cI.get.mean
          k -= nU * kI
          break
        }

        t += kI
      }
    }
    s / k
  }

  def interpolate(i: Int, key: Double, diff: Double): Double = {
    val cI = this.centroids.get(key)
    val kI = cI.get.count

    val delta = 
      if (i == 0) 
        this.centroids.get(successorKey(key)).get.mean - cI.get.mean
      else if (i == len() - 1) 
        cI.get.mean - this.centroids.get(previousKey(key)).get.mean
      else
        (this.centroids.get(successorKey(key)).get.mean - this.centroids.get(previousKey(key)).get.mean) / 2.0

    (diff / kI - 0.5) * delta
  }

  def empty() = {
    this.centroids.clear
    this.totalCount = 0
  }
}

object TDigest {
  def main(args: Array[String]):Unit = {
    var s: Seq[Double] = (0 to Random.nextInt(10000)).map(i => Random.nextDouble)

    var TD = new TDigest()
    val x = Seq(s, Seq(0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0))
    x.foreach { x =>
      TD.empty
      TD.batchUpdate(x)
      println(s"DataSetSize = ${x.size}")
      println(s"TDigest Len (compression) = ${TD.len}")

      println(Math.abs(TD.percentile(50) - 0.5))
      println(Math.abs(TD.percentile(10) - 0.1))
      println(Math.abs(TD.percentile(90) - 0.9))
      println(Math.abs(TD.percentile(1) - 0.01))
      println(Math.abs(TD.percentile(99) - 0.99))
      println(Math.abs(TD.percentile(0.1) - 0.001))
      println()
      println(Math.abs(TD.trimmedMean(0.5 ,1.0)))
      println("------")
    }


    TD.empty
    TD.batchUpdate(Seq(1.0, 2.0, 3.0, 3.0))
    TD.centroids.foreach {
      c => println(s"Key: ${c._1} -- Centroid - mean: ${c._2.mean} - count: ${c._2.count}")
    }
    println(TD)
    println(s"Median: ${Math.abs(TD.percentile(50))}")
    println("------")
    TD.empty
    TD.batchUpdate(Seq(1,2,2,2,2,2,2,2,3))
    TD.centroids.foreach {
      c => println(s"Key: ${c._1} -- Centroid - mean: ${c._2.mean} - count: ${c._2.count}")
    }
    println(TD)
    println(s"Median: ${Math.abs(TD.percentile(50))}")


    TD.empty
    TD.batchUpdate(Seq(62.0, 202.0, 1415.0, 1433.0))
    println(TD)
    println(Math.abs(TD.percentile(.25)))
  }
}