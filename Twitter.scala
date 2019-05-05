import org.apache.spark.SparkConf
import org.apache.spark.sql.Column
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.types.{StringType, StructField, StructType,IntegerType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._


object Twitter {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Twitter")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val schema =new StructType().add(StructField("handle",StringType,true)).add(StructField("location",StringType,true)).add(StructField("language",StringType,true)).add(StructField("posts",IntegerType,true)).add(StructField("followers",IntegerType,true)).add(StructField("following",IntegerType,true)).add(StructField("hashtags",StringType,true))

    
    val df = spark.read.option("quote", "[").option("ignoreLeadingWhiteSpace",true).schema(schema).csv("s3://bucket_name/TwitterApp2019/01/08/14/").toDF(
    "handle",
    "location",
    "language",
    "posts",
    "followers",
    "following",
    "hashtags")

    //val esdf = sqlContext.read.format("com.databricks.spark.csv").option("header","false").option("inferSchema", "false").schema(rfSchema).load("/user/horf/horf_flag_rules.csv")
    val df2 = df.withColumn("hashtags",split(regexp_replace('hashtags,"[]]",""),","))

    df2.registerTempTable("push")
    
    val first=spark.sql("select handle ,followers from push group by handle,followers order by followers desc").show

    val second=spark.sql("select language,count(language) as pop from push group by language order by pop desc").show

    val third=spark.sql("select location,count(location) as pop from Push group by location order by pop desc").show

    val fourth=spark.sql("select hashtags,location, count(hashtags) as hs from Push group by hashtags,location order by hs desc").show



    val fifth=spark.sql("SELECT  value, count(*) AS count FROM (SELECT explode(hashtags) AS value FROM push) AS temp GROUP BY value ORDER BY count DESC").show
  }
  
}