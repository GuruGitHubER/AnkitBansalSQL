package AnkitBansal

// Values in df1 that are not in df2 -- using join

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



object JoinWhereCondition {

	def main(args:Array[String]):Unit={

			val conf = new SparkConf().setAppName("SparkIntPrac").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder.getOrCreate()
					import spark.implicits._

					val data1 = Seq((1,"Arun"),
							(2,"Karthi"),
							(3,"Guru"),
							(4,"Siva"),
							(5,"Tilak"),
							(6,"Mappi"),
							(7,"Yoga"),
							(8,"Saravana"),
							(9,"Sam"))
							
				val data2 = Seq((1,"Arun"),
							(2,"Karthi"),
							(3,"Guru"),
							(4,"Siva")
						)
						
				val df1 = data1.toDF("id","name")
				
				val df2 = data2.toDF("id","name")
				
				df1.show()
				df2.show()
				
				df1.createOrReplaceTempView("df1TmpVw")
				df2.createOrReplaceTempView("df2TmpVw")
				
				val df3 =spark.sql("""
				                select a.id, a.name, b.id from df1TmpVw a
				                left join df2TmpVw b
				                on a.id = b.id
				                where b.id IS NULL			  
				  
				                  """)

  df3.show()








	}

}