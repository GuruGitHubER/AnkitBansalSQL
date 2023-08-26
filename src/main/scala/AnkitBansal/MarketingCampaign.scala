package AnkitBansal

import org.apache.spark.SparkContext  // rdd
import org.apache.spark.sql.SparkSession  // dataframe
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window

object MarketingCampaign {

	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("SparkIntPrac").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder.getOrCreate()
					import spark.implicits._


					val df = spark.read
					.format("csv")
					.option("header","true")
					.load("file:///C:/data/Ankit Bansal Data/MarketingCampaignData.csv")


					df.show()
					df.printSchema()


					//val windowspec1 = Window.partitionBy("user_id","Ceated_at").orderby(desc("product")

					val windowspec1 = Window.partitionBy("User_id").orderBy("Created_at")
					
					

					val df1 = df.withColumn("Created_at", to_date(col("Created_at"),"yyyy-MM-dd"))
					            .withColumn("price",col("price").cast("int"))
					            .withColumn("Qnty",col("Qnty").cast("int"))

				/*	df1.show()
					df1.printSchema()*/

      val df2 = df1.groupBy("User_id", "Created_at").agg(sum(col("price")).alias("price"))
      
      df2.show()
      
					val df3 = df2.withColumn("Dense_Rank", dense_rank().over(windowspec1))
					
					df3.show()

					//df3.where("Dense_Rank > 1").show()


	}

}