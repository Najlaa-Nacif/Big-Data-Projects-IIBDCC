package ma.enset;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLApp {
    public static void main(String[] args) throws AnalysisException {
        SparkSession ss = SparkSession
                .builder()
                .appName("SparkSQLApp")
                .master("local[*]").getOrCreate();

        // 1. Data Loading & Exploration
        Dataset<Row> bikeRentals = ss.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("/opt/spark-apps/rentals (1).csv");

        bikeRentals.printSchema();
        bikeRentals.show(5);
        System.out.println("Total number of rentals: " + bikeRentals.count());

        // 2. Create Temporary View
        bikeRentals.createOrReplaceTempView("bike_rentals_view");

        // 3. Basic SQL Queries
        System.out.println("-- Rentals longer than 30 minutes --");
        ss.sql("SELECT * FROM bike_rentals_view WHERE duration_minutes > 30").show();

        System.out.println("-- Rentals starting at 'Station Centre-Ville' --");
        ss.sql("SELECT * FROM bike_rentals_view WHERE start_station = 'Station Centre-Ville'").show();

        System.out.println("-- Total revenue --");
        ss.sql("SELECT sum(price) AS total_revenue FROM bike_rentals_view").show();

        // 4. Aggregation Queries
        System.out.println("-- Number of rentals per start station --");
        ss.sql("SELECT start_station, count(*) AS nb_rentals " +
                "FROM bike_rentals_view GROUP BY start_station").show();

        System.out.println("-- Average rental duration per start station --");
        ss.sql("SELECT start_station, avg(duration_minutes) AS avg_duration " +
                "FROM bike_rentals_view GROUP BY start_station").show();

        System.out.println("-- Station with the highest number of rentals --");
        ss.sql("SELECT start_station, count(*) AS nb_rentals " +
                "FROM bike_rentals_view GROUP BY start_station " +
                "ORDER BY nb_rentals DESC LIMIT 1").show();

        // 5. Time-Based Analysis
        System.out.println("-- Hour extracted from start_time --");
        ss.sql("SELECT *, hour(start_time) AS start_hour FROM bike_rentals_view").show(5);

        System.out.println("-- Number of rentals per hour (peak hours) --");
        ss.sql("SELECT hour(start_time) AS start_hour, count(*) AS nb_rentals " +
                "FROM bike_rentals_view GROUP BY hour(start_time) ORDER BY start_hour").show(24);

        System.out.println("-- Most popular start station in the morning (7-12) --");
        ss.sql("SELECT start_station, count(*) AS nb_rentals " +
                "FROM bike_rentals_view " +
                "WHERE hour(start_time) >= 7 AND hour(start_time) < 12 " +
                "GROUP BY start_station ORDER BY nb_rentals DESC LIMIT 1").show();

        // 6. User Behavior Analysis
        System.out.println("-- Average age of users --");
        ss.sql("SELECT avg(age) AS avg_age FROM bike_rentals_view").show();

        System.out.println("-- Number of users per gender --");
        ss.sql("SELECT gender, count(*) AS nb_users FROM bike_rentals_view GROUP BY gender").show();

        System.out.println("-- Rentals per age group --");
        ss.sql("SELECT " +
                "CASE " +
                "  WHEN age BETWEEN 18 AND 30 THEN '18-30' " +
                "  WHEN age BETWEEN 31 AND 40 THEN '31-40' " +
                "  WHEN age BETWEEN 41 AND 50 THEN '41-50' " +
                "  ELSE '51+' " +
                "END AS age_group, " +
                "count(*) AS nb_rentals " +
                "FROM bike_rentals_view " +
                "GROUP BY age_group ORDER BY nb_rentals DESC").show();

        ss.stop();
    }
}
