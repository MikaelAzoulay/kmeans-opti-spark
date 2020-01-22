import java.lang.Boolean

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object KM_Scala {

  def computeDistance(x : List[Float], y : List[Float]): Float ={
    val  result : List[Float] = for{
      (a, b) <- x zip y
    } yield (a-b)*(a-b)
    result.sum
  }

  def closestCluster(distList : List[(Long, Float)]) : (Long, Float) = {
    // scala.util.Sorting.stableSort(distList, | (e1 : (Long, Float), e2 : ((Long, Float)) => e1._2 < e2._2 ))
    (distList.sortBy(_._2)).head
  }

  def sumList(x : List[Float], y : List[Float]) : List[Float] = {
    x.zip(y).map(x=>x._1 + x._2)
  }

  def moyenneList(x : List[Float], n : Int) : List[Float] = {
    x.map(_/n)
  }

  def permute(l : (Any, Any)) : (Any, Any) = {
    // val s = for (x <- l) yield (x._2, x._1)
    (l._2, l._1)
  }

  def permute(l : List[(Int, Int)]) : List[(Int, Int)] = {
    val s = for (x <- l) yield (x._2, x._1)
    return s
  }

  def simpleKMeans(sc : SparkContext, data : RDD[(Long, List[String])], nbClusters : Int) :
                            Tuple4[RDD[(Long, ((Long, Float), List[String]))], Double, Int, Long] = {

    var timeStart = System.currentTimeMillis()
    var clusteringDone = Boolean.FALSE
    var numberOfSteps : Int = 0
    var currentError : Double = Double.MinValue
    var numberOfElements : Broadcast[Long] = sc.broadcast(data.count())
    var assignment : RDD[(Long, ((Long, Float), List[String]))] = sc.emptyRDD

    // Select initial centroides
    var centroides = sc.parallelize(data.takeSample(withReplacement = Boolean.FALSE, nbClusters))
                                .zipWithIndex()
                                .map(x => (x._2, x._1._2.dropRight(1)))
    //centroides.collect().foreach(println)
    centroides.saveAsTextFile("centroidesIni")

    var i : Int = 0
    while (!clusteringDone){
      var joined = data.cartesian(centroides)
      //joined.take(10).foreach(println)
      joined.saveAsTextFile("joined" + i )
      val joined2 = joined.map(x => (x._1._1, (x._2._1, (x._1._2.dropRight(1).map(_.toString.toFloat),
        x._2._2.map(_.toString.toFloat)))))
      //joined2.take(10).foreach(println)
      joined2.saveAsTextFile("joined2" + i)

      var distances = joined2.map(x => (x._1, (x._2._1, computeDistance(x._2._2._1, x._2._2._2))))
      //distances.take(10).foreach(println)
      distances.saveAsTextFile("distances" + i)

      var distancesList = distances.groupByKey().map(x => (x._1, x._2.toList))
      //distancesList.take(10).foreach(println)
      distancesList.saveAsTextFile("distancesList" + i)

      var minDist = distancesList.mapValues(x => closestCluster(x))
      //minDist.take(15).foreach(println)
      minDist.saveAsTextFile("minDist" + i)

      var assignment = minDist.join(data)
      //assignment.take(10).foreach(println)
      assignment.saveAsTextFile("assignment" + i)

      var clusters = assignment.map(x => (x._2._1._1, x._2._2.dropRight(1)))
      //clusters.take(10).foreach(println)
      clusters.saveAsTextFile("clusters" + i)

      var count = clusters.map(x => (x._1, 1)).reduceByKey((x, y) => x+y)
      //count.take(10).foreach(println)

      // println(sumList(List(2, 3), List(4, 6)))
      var somme = clusters.reduceByKey((x, y) => sumList(x.map(_.toString.toFloat), y.map(_.toString.toFloat)).map(_.toString))
      //somme.take(10).foreach(println)

      var centroidesCluster = somme.join(count).map(x => (x._1, moyenneList(x._2._1.map(_.toString.toFloat), x._2._2)map(_.toString())))
      //centroidesCluster.take(10).foreach(println)
      centroidesCluster.saveAsTextFile("centroidesCluster" + i)

      var switch : Int = 0
      var prevAssignment : RDD[(Long, (Long, Float))] = sc.emptyRDD
      if (numberOfSteps > 0){
        switch = prevAssignment.join(minDist).filter(x => x._2._1._1 != x._2._2._1).count().toString.toInt
        //println(switch)
      }else{
        switch = 150
      }

      if (switch == 0 || numberOfSteps == 5){
        clusteringDone = Boolean.TRUE
        currentError = Math.sqrt(minDist.map(x => x._2._2).reduce((x,y) => x+y)) / numberOfElements.value
      } else{
        centroides = centroidesCluster
        prevAssignment = minDist
        numberOfSteps += 1
      }

      i += 1
    } // while

    var timeEnd = System.currentTimeMillis()

    (assignment, currentError, numberOfSteps+1, timeEnd - timeStart)

  }

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:/hadoop")

    val conf = new SparkConf()
    conf.setMaster("local")
    conf.setAppName("KM")
    val sc = new SparkContext(conf)

    // Load the text into a Spark RDD, which is a distributed representation of each line of text
    val textFile = sc.textFile("C:\\Users\\Marc\\IdeaProjects\\HelloScala\\src\\main\\resources\\iris.data.txt")

    val data = textFile.map(line => line.split(","))
      // .map(x => x(4))
      .map(x => List.concat(x.splitAt(4)._1, List(x(4))))
      .zipWithIndex()
      .map(x => (x._2, x._1))
    //data.collect().foreach(println)
    var resultats = simpleKMeans(sc, data, 3)
    println("Affichage des résultats")
    println("==============================================================================")
    resultats._1.take(10).foreach(println)
    println("Error : "  +resultats._2)
    println("Number of steps : "  + resultats._3)
    println("Temps d'exécution : " + resultats._4 + "ms")

  }

}