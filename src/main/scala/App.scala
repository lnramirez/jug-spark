import java.time.{Duration, Instant}

import org.apache.spark.sql.SparkSession

import scala.io.Source

object App  {

  def main(args : Array[String]): Unit = {
    val instant = Instant.now()
    // Spark session available as 'spark'.

    val spark = SparkSession.builder().appName("githubarchive")
      .master("local[*]").getOrCreate()

    // Spark context available as 'sc'
    val sc = spark.sparkContext

    def myEmp = Source.fromFile("/Users/luisramirez/prj/github-archive/employees-id.csv").getLines.toSet
    val broadcastEmployees = sc.broadcast(myEmp)
    val isInEmployee = anId => broadcastEmployees.value.contains(anId)
    val isEmp = spark.udf.register("SetContainsId", isInEmployee)

    val allRecs = spark.read.json("/Users/luisramirez/prj/github-archive/*.json")

    import spark.implicits._

    val filtered = allRecs.filter(isEmp($"actor.id"))

    filtered.repartition(1).write.json("./out/myemps")
    println(s"${Duration.between(instant, Instant.now()).toMillis}")
  }

}
