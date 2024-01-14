package ma.enset.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SparkSQL {
    public static void main(String[] args) {
        SparkSession ss= SparkSession.builder().appName("TP SPARK SQL").master("local[*]").getOrCreate();
        // Configurer les détails de la connexion MySQL
        String url = "jdbc:mysql://localhost:4306/DB_AEROPORT";
        Properties props = new Properties();
        props.setProperty("user", "root");
        props.setProperty("password", "");

        // Charger les données depuis MySQL dans des DataFrames
        Dataset<Row> volsDF = ss.read().jdbc(url, "VOLS", props);
        Dataset<Row> passagersDF = ss.read().jdbc(url, "PASSAGERS", props);
        Dataset<Row> reservationsDF = ss.read().jdbc(url, "RESERVATIONS", props);
        volsDF.show();
        passagersDF.show();
        reservationsDF.show();
        // Afficher pour chaque vol, le nombre de passagers
        Dataset<Row> nombrePassagersParVol = volsDF
                .join(reservationsDF, volsDF.col("ID").equalTo(reservationsDF.col("ID_VOL")))
                .join(passagersDF, passagersDF.col("ID").equalTo(reservationsDF.col("ID_PASSAGER")))
                .groupBy("ID_VOL", "DATE_DEPART")
                .agg(org.apache.spark.sql.functions.count(passagersDF.col("ID")).alias("NOMBRE"));

        System.out.println("1. Afficher pour chaque vol, le nombre de passagers :");
        nombrePassagersParVol.show();

        // Afficher la liste des vols en cours
        Dataset<Row> volsEnCours = volsDF
                .filter("CURRENT_DATE() BETWEEN DATE_DEPART AND DATE_ARRIVE");

        System.out.println("2. Afficher la liste des vols en cours :");
        volsEnCours.show();

        ss.stop();

    }
}
