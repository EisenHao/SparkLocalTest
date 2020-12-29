import scala.math.random
import org.apache.spark.sql.SparkSession

/**
 * Spark 简易程序：计算圆周率 Pi
 *
 * 原理：随机落在与正方形相切的同心圆内的概率为：S圆 / S正 = Pi / 4
 * 备注：当随机抛点次数大于一百万时，才具有参考意义
 */
object SparkPi {
    def main(args: Array[String]) {

        val spark = SparkSession
          .builder
          .appName("Spark Pi")
          .getOrCreate()
        val slices = if (args.length > 0) args(0).toInt else 2
        val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
        val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
            val x = random * 2 - 1
            val y = random * 2 - 1
            if (x * x + y * y < 1) 1 else 0
        }.reduce(_ + _)
        println("Pi is roughly " + 4.0 * count / (n - 1))
        spark.stop()
    }
}
