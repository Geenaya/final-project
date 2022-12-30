import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main extends App {

    val spark = SparkSession.builder
                            .master("local[*]")
                            .appName("final-project")
                            .getOrCreate()

    val datacsv = spark.read
                       .options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true"))
                       .csv("src/main/data/yellow_tripdata_2020-01.csv")

    val datacsv0 = datacsv.groupBy( "VendorID",
                                    "tpep_pickup_datetime",
                                          "tpep_dropoff_datetime",
                                          "passenger_count",
                                          "trip_distance")
                          .agg(sum("tip_amount").as("tip_amount"),
                  sum("total_amount").as("total_amount"))

    val datacsv1 = datacsv0.withColumn("date", to_date(col("tpep_dropoff_datetime"), "yyyy-MM-dd"))
                           .withColumn("passanger", expr("case when passenger_count < 4 then passenger_count else 4 end"))

    val df = datacsv1.select( col("date"),
                              col("passanger"),
                              col("total_amount"),
                              col("trip_distance"),
                              col("passenger_count"),
                              col("tip_amount"))
                      .where(col("total_amount") >= 0)

    df.write
      .partitionBy("date")
      .parquet("src/main/data/data.parquet")

    val parquet_data = spark.read
                            .parquet("src/main/data/data.parquet")
                            .where("date >= '2020-01-01' and date <= '2020-01-31'  ")
    parquet_data.createOrReplaceTempView("ParquetTable2")
    val parquet_data0 = spark.sql("select * from ParquetTable2")

    val tab_data = parquet_data0.select("*")
                                .where(col("trip_distance") > 0)

    val group_data1 = tab_data.groupBy("date", "passanger")
                              .agg( count("*").as("p_count"),
                                    max("total_amount").as("max_total_amount"))

    val group_data2 = tab_data.groupBy("date").count()

    val group_data3 = ( tab_data.select("date", "passanger", "total_amount")
                                .where(col("total_amount") > 0))
                                .groupBy("date", "passanger")
                                .min("total_amount")

    val join_data1 = group_data1.join(group_data2, "date")
                                .select(group_data1.col("date"),
                                        group_data1.col("passanger"),
                                        group_data1.col("p_count"),
                                        group_data2.col("count").as("t_count"),
                                        group_data1.col("max_total_amount"))
                                .withColumn("persent", round(col("p_count") / col("t_count") * 100, 2))

    val join_data2 =  join_data1.join(group_data3, Seq("date", "passanger"), "left")
                                .select(join_data1.col("date"),
                                        join_data1.col("passanger"),
                                        join_data1.col("p_count"),
                                        join_data1.col("t_count"),
                                        join_data1.col("persent"),
                                        join_data1.col("max_total_amount"),
        group_data3.col("min(total_amount)").as("min_total_amount"))

    val pivotDF = join_data2.groupBy("date")
                            .pivot("passanger")
                            .agg( first("persent").as("persent"),
                                  first("max_total_amount").as("max_ta"),
                                  first("min_total_amount").as("min_ta"))

    val persent_trip = pivotDF.select(col("date"),

                                      col("0_persent").as("percentage_zero"),
                                      col("1_persent").as("percentage_1p"),
                                      col("2_persent").as("percentage_2p"),
                                      col("3_persent").as("percentage_3p"),
                                      col("4_persent").as("percentage_4p_plus"),

                                      col("0_max_ta").as("max_total_amount_zero"),
                                      col("1_max_ta").as("max_total_amount_1p"),
                                      col("2_max_ta").as("max_total_amount_2p"),
                                      col("3_max_ta").as("max_total_amount_3p"),
                                      col("4_max_ta").as("max_total_amount_4p_plus"),

                                      col("0_min_ta").as("min_total_amount_zero"),
                                      col("1_min_ta").as("min_total_amount_1p"),
                                      col("2_min_ta").as("min_total_amount_2p"),
                                      col("3_min_ta").as("min_total_amount_3p"),
                                      col("4_min_ta").as("min_total_amount_4p_plus"))

    persent_trip.write
                .parquet("src/main/data/taxitripdata.parquet")

    val parquetDFall_show = spark.read.parquet("src/main/data/taxitripdata.parquet")
    parquetDFall_show.createOrReplaceTempView("ParquetTable3")
    val persent_trip_show = spark.sql("select * from ParquetTable3")
    persent_trip_show.orderBy(asc("date")).show(50)

    val parquetDFall = spark.read
                            .parquet("src/main/data/data.parquet")
                            .where("date >= '2020-01-01' and date <= '2020-01-31'  ")
    parquetDFall.createOrReplaceTempView("ParquetTable2")
    val graf_data = spark.sql("select * from ParquetTable2")

    val data_grafic =  graf_data.select("trip_distance", "passenger_count", "tip_amount")
                                .distinct()
                                .where(col("trip_distance") > 0
                                    && col("trip_distance") < 61
                                    && col("passenger_count") > 0
                                    && col("tip_amount") < 251)
                                .withColumn("trip_distance_round", round(col("trip_distance"), 0))
                                .withColumn("tip_amount_round", round(col("tip_amount"), 0))

    data_grafic.coalesce(1)
               .write
               .option("header", "true")
               .option("delimiter", ",")
               .mode("overwrite")
               .csv("src/main/data/project.csv")
}