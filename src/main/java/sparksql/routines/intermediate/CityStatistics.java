package sparksql.routines.intermediate;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/intermediate/city_statistics

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class CityStatistics {

    public static void main(String[] args) {

        if (args.length < 2) {
            System.err.println("Usage: CityStatistics <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessão Spark
        SparkSession spark = SparkSession.builder()
                .appName("CityStatistics")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Lê o arquivo CSV
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        // Calcula estatísticas por cidade: contagem, total e média
        Dataset<Row> result = transactions
                .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
                .groupBy("merchant_city")
                .agg(
                        count("*").as("TransactionCount"),
                        round(sum("amount_clean"), 2).as("TotalAmount"),
                        round(avg("amount_clean"), 2).as("AverageAmount")
                )
                .orderBy(desc("TotalAmount"));

        // Mostra 20 resultados no console
        System.out.println("Exibindo estatísticas por cidade:");
        result.show(20, false);

        // Estatisticas globais
        long totalCities = result.count();
        Row globalStats = result.agg(
                sum("TransactionCount").alias("total_transactions"),
                sum("TotalAmount").alias("total_amount")
        ).first();

        System.out.println("\nEstatísticas Globais:");
        System.out.println("  Total de cidades: " + totalCities);
        System.out.println("  Total de transações: " + globalStats.getLong(0));
        System.out.println("  Valor total: $" + String.format("%.2f", globalStats.getDouble(1)));

        // Salva resultado
        result.coalesce(1)
                .write()
                .option("header", "true")
                .mode("overwrite")
                .csv(outputPath);

        System.out.println("\nResultados salvos em: " + outputPath);

        spark.stop();
    }
}
