package ma.enset.spark.streaming;

import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.functions;

import java.util.concurrent.TimeoutException;

public class StreamingApp {

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("Incident Analysis")
                .master("local[2]")
                .getOrCreate();

        // Define the schema for your CSV data
        StructType schema = new StructType()
                .add("id", DataTypes.IntegerType, false, Metadata.empty())
                .add("description", DataTypes.StringType, false, Metadata.empty())
                .add("no_avion", DataTypes.StringType, false, Metadata.empty())
                .add("date", DataTypes.DateType, false, Metadata.empty());

        // Lecture des données en streaming à partir du répertoire spécifié
        Dataset<Row> incidents = spark.readStream()
                .option("header", true)
                .schema(schema)
                .csv("hdfs://localhost:9000/streaming");

        // Question 1: Afficher d’une manière continue l’avion ayant plus d’incidents.
        StreamingQuery queryAvionPlusIncidents = incidents.groupBy("no_avion")
                .count()
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Question 2: Afficher d’une manière continue les deux mois de l’année en cours où il y avait moins d’incidents.
        StreamingQuery queryMoisMoinsIncidents = incidents.groupBy(functions.year(incidents.col("date")).as("year"),
                        functions.month(incidents.col("date")).as("month"))
                .count()
                .filter("count < 5") // Vous pouvez ajuster le seuil selon votre critère
                .writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Attendre la fin du streaming
        queryAvionPlusIncidents.awaitTermination();
        queryMoisMoinsIncidents.awaitTermination();
    }
}
