package ma.enset.spark.csv;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.year;


public class IncidentAnalysis {

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("Incident Analysis")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df1=ss.read().option("header",true).csv("incidents2.csv");
        System.out.println("****************************************************************");
        System.out.println("--------------------------- DataSet------------------------------");
        df1.show();
        System.out.println("******************************************************************");
        System.out.println("1. Afficher le nombre d’incidents par service.");
        df1.groupBy(col("service")).count().show();
        System.out.println("*******************************************************************");
        System.out.println("2. Afficher les deux années où il a y avait plus d’incidents.");
        df1.groupBy(year(col("date"))).count().orderBy(col("count").desc()).show(2);

    }
}
