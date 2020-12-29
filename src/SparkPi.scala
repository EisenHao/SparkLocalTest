import scala.math.random
import org.apache.spark.sql.SparkSession

/**
 * Spark 简易程序：根据大数定律，以概率求计算圆周率 Pi
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
        // 几何解释：随机点(x,y)，散落在以原点为几何中心边长为2的正方形中，统计落在以原点为圆心半径为1的圆内的点数
        val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
            val x = random * 2 - 1
            val y = random * 2 - 1
            if (x * x + y * y < 1) 1 else 0
        }.reduce(_ + _)
        // 根据大数定律，当n足够大时，落在第一象限的 1/4 圆内点概率为：S(1/4圆) / S(1*1) = Pi / 4，可间接求 Pi
        println("Pi is roughly " + 4.0 * count / (n - 1))
        spark.stop()
    }
}
