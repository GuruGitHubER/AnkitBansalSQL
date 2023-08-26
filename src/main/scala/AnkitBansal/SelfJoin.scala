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


object SelfJoin {
	def main(args:Array[String]):Unit={
			val conf = new SparkConf().setAppName("SparkIntPrac").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder.getOrCreate()
					import spark.implicits._

					val data = Seq((1,"Arun",2),
							(1,"Arun",3),
							(1,"Arun",4),
							(2,"Preethi",1),
							(2,"Preethi",3),
							(3,"Prem",4),
							(3,"Prem",1),
							(4,"Akshay",1),
							(4,"Akshay",2)
							) 
							
				val data1 = Seq((1,"Arun"),
							(2,"Preethi"),
							(3,"Prem"),
							(4,"Akshay"))
							
				val df4 = spark.createDataFrame(sc.parallelize(data1)).toDF("id","name")
							
				val df1 = spark.createDataFrame(sc.parallelize(data)).toDF("id","name","Liked_id")
				
				df1.show()
				
				df1.printSchema()
				
				val df2= df1.as("d1").join(df1.as("d2"), col("d1.Liked_id") === col("d2.id"),"inner")
				
				df2.show()
				
				val df3 = df2.select(col("d1.id"), col("d1.name"), col("d2.id"),col("d2.name"))
				
			df3.distinct().orderBy("d1.id").show()
			
			////Not Yet Solved -- Have to Complete --- ///////////
			
			
	} 
}