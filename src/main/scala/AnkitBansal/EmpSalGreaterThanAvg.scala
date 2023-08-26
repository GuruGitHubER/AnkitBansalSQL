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
import org.apache.spark.sql.expressions.Window


object EmpSalGreaterThanAvg {
  
  def main(args:Array[String]):Unit={
    
       val conf = new SparkConf().setAppName("Revision").setMaster("local[*]")
					val sc = new SparkContext(conf)
					sc.setLogLevel("ERROR")

					val spark = SparkSession.builder().getOrCreate()
					import spark.implicits._
					
					
					
					val data =Seq(
					(1,101,5000),
					(2,101,6000),
					(3,102,8000),
					(4,102,6000),
					(5,101,7000),
					(6,102,5000),
					(7,102,7000))
					
					val df = data.toDF("emp_id","dep_id","Sal")
					
					df.orderBy("dep_id","emp_id").show()
	
    val df1 = df.groupBy("dep_id").agg(avg("Sal").cast(IntegerType).alias("Avg_Sal"))
    
    df1.show()
    
    val df2 = df.join(df1, Seq("dep_id"),"left")
    .where("Sal > Avg_sal")
    
    df2.show()
    
    
    
  }
  
}