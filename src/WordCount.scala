import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

// 使用 IDE 完成 WordCount 开发&测试\
object WordCount {
    def main(args: Array[String]): Unit = {

        // 创建 SparkConf 配置对象，设定Spark 计算框架的运行环境(local[*])
        val sparkCfg: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")

        // 创建 scala 上下文对象
        val sc = new SparkContext(sparkCfg)

        // 逐行读文件
        val lines: RDD[String] = sc.textFile("in")

        // 分解成单词
        val words: RDD[String] = lines.flatMap(_.split(" "))

        // 映射为元组：拼接成(单词, 1)元组
        val wordTuple: RDD[(String, Int)] = words.map((_, 1))

        // 按单词聚合（次数相加）
        val wordTupleSum = wordTuple.reduceByKey(_ + _)

        // 采集结果
        val result: Array[(String, Int)] = wordTupleSum.collect()

        // 打印结果
        result.foreach(println)
    }
}
