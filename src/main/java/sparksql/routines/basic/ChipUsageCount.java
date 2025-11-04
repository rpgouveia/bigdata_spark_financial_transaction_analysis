package sparksql.routines.basic;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/basic/chip_usage_count

/**
 * Rotina básica que conta os tipos de transação (chip vs swipe).
 * Agrupa transações pelo campo use_chip após normalização.
 */
public class ChipUsageCount {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ChipUsageCount <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessão Spark
        SparkSession spark = SparkSession.builder()
                .appName("ChipUsageCount")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Lê o arquivo CSV
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        // Limpa o campo, remove espaços, aspas e converte para maiúsculo
        Column cleanChip = upper(regexp_replace(trim(col("use_chip")), "\"", ""));

        Column transactionType =
                when(col("use_chip").isNull()
                        .or(trim(col("use_chip")).equalTo("")), "Unknown Transaction")
                        .when(cleanChip.contains("TRANSACTION"), initcap(lower(cleanChip)))
                        .when(cleanChip.isin("Y", "YES", "TRUE", "1"), "Chip Transaction")
                        .when(cleanChip.isin("N", "NO", "FALSE", "0"), "Swipe Transaction")
                        .when(cleanChip.equalTo("ONLINE"), "Online Transaction")
                        .when(cleanChip.isin("NULL", "N/A"), "Unknown Transaction")
                        // default: capitaliza o valor e adiciona " Transaction"
                        .otherwise(concat(initcap(lower(cleanChip)), lit(" Transaction")));

        // Aplica a transformação, agrupa, conta e ordena
        Dataset<Row> result = transactions
                .withColumn("TransactionType", transactionType)
                .groupBy("TransactionType")
                .count()
                .orderBy(desc("count"));

        // Mostra os resultados no console
        result.show(false);

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