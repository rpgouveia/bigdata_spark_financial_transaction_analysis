package sparksql.routines.basic;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/basic/amount_by_client

/**
 * Rotina básica que calcula o valor total transacionado por cliente.
 * Agrupa transações por client_id e soma os valores.
 */

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class AmountByClient {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: AmountByClient <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessão Spark
        SparkSession spark = SparkSession.builder()
                .appName("AmountByClient")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Lê o arquivo CSV
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        // Remove o cifrão, converte para double, agrupa por client_id, soma os valores e ordena
        Dataset<Row> result = transactions
                .withColumn("amount_clean", regexp_replace(col("amount"), "\\$", "").cast("double"))
                .groupBy("client_id")
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
