package sparksql.routines.basic;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/basic/amount_by_city

/**
 * Rotina básica que calcula o valor total transacionado por cidade.
 * Agrupa transações por merchant_city e soma os valores.
 */
public class AmountByCity {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: AmountByCity <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessão Spark
        SparkSession spark = SparkSession.builder()
                .appName("AmountByCity")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Lê o arquivo CSV
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        // Remove o cifrão, converte para double, agrupa por cidade, soma os valores e ordena
        Dataset<Row> result = transactions
                .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
                .groupBy("merchant_city")
                .agg(round(sum("amount_clean"), 2).as("TotalAmount"))
                .orderBy(desc("TotalAmount"));

        // Mostra os resultados no console
        result.show(20, false);

        // Salva os resultados em CSV
        result.coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("Results saved to: " + outputPath);

        spark.stop();
    }
}