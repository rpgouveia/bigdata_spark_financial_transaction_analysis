package sparksql.routines.intermediate.citystatistics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/intermediate/city_statistics local

/**
 * CityStatistics usando Apache Spark SQL (DataFrames)
 *
 * Demonstra o uso de DataFrames e operações declarativas do Spark SQL
 * para calcular estatísticas completas por cidade:
 * - Número total de transações
 * - Valor total transacionado
 * - Valor médio por transação (ticket médio)
 */
public class CityStatistics {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Iniciando CityStatistics com Spark SQL...");
        System.out.println("Rotina Intermediária - DataFrames");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Objetivo: Calcular estatísticas completas por cidade");
        System.out.println("  - Número de transações");
        System.out.println("  - Valor total transacionado");
        System.out.println("  - Ticket médio (valor médio por transação)");
        System.out.println();

        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: CityStatisticsSQL <input_path> <output_path> [local]");
            System.err.println("  input_path: caminho do arquivo CSV de transações");
            System.err.println("  output_path: caminho do diretório de saída");
            System.err.println("  local: para execução local (opcional)");
            System.exit(-1);
        }

        // Parse dos parâmetros
        String inputPath = args[0];
        String outputPath = args[1];
        boolean localMode = (args.length > 2 && "local".equals(args[2]));

        // Configurar SparkSession
        SparkSession.Builder sparkBuilder = SparkSession.builder()
                .appName("CityStatistics-SparkSQL");

        if (localMode) {
            System.out.println("Configurando para execução local...");
            sparkBuilder.master("local[*]");
        }

        SparkSession spark = sparkBuilder.getOrCreate();

        // Configurar nível de log
        spark.sparkContext().setLogLevel("WARN");

        try {
            System.out.println("========================================");
            System.out.println("Configuração do Job:");
            System.out.println("  Mode: " + (localMode ? "Local" : "Cluster"));
            System.out.println("  Input: " + inputPath);
            System.out.println("  Output: " + outputPath);
            System.out.println("  Engine: Spark SQL (DataFrames)");
            System.out.println("========================================");
            System.out.println();

            long startTime = System.currentTimeMillis();

            // Definir schema do CSV para melhor performance e type safety
            StructType schema = new StructType()
                    .add("id", DataTypes.StringType, true)
                    .add("date", DataTypes.StringType, true)
                    .add("client_id", DataTypes.StringType, true)
                    .add("card_id", DataTypes.StringType, true)
                    .add("amount", DataTypes.StringType, true)
                    .add("use_chip", DataTypes.StringType, true)
                    .add("merchant_id", DataTypes.StringType, true)
                    .add("merchant_city", DataTypes.StringType, true)
                    .add("merchant_state", DataTypes.StringType, true)
                    .add("zip", DataTypes.StringType, true)
                    .add("mcc", DataTypes.StringType, true)
                    .add("errors", DataTypes.StringType, true);

            // Ler CSV com schema definido
            System.out.println("Lendo arquivo CSV...");
            Dataset<Row> transactionsDF = spark.read()
                    .option("header", "true")
                    .option("quote", "\"")
                    .option("escape", "\"")
                    .schema(schema)
                    .csv(inputPath);

            long totalRecords = transactionsDF.count();
            System.out.println("Total de registros lidos: " + totalRecords);
            System.out.println();

            // Processar dados: limpar e transformar
            System.out.println("Processando e agregando dados...");
            Dataset<Row> processedDF = transactionsDF
                    // Limpar cidade: remover aspas, uppercase, tratar nulls
                    .withColumn("clean_city",
                            when(col("merchant_city").isNull()
                                            .or(trim(col("merchant_city")).equalTo(""))
                                            .or(upper(trim(col("merchant_city"))).equalTo("NULL"))
                                            .or(upper(trim(col("merchant_city"))).equalTo("N/A")),
                                    lit("UNKNOWN"))
                                    .otherwise(upper(trim(regexp_replace(col("merchant_city"), "\"", "")))))
                    // Converter amount para double: remover $, virgulas e converter
                    .withColumn("clean_amount",
                            regexp_replace(
                                    regexp_replace(
                                            regexp_replace(col("amount"), "\\$", ""),
                                            "\"", ""),
                                    ",", "")
                                    .cast(DataTypes.DoubleType))
                    // Filtrar registros válidos
                    .filter(col("clean_amount").isNotNull()
                            .and(col("clean_amount").gt(0)));

            // Calcular estatísticas por cidade usando agregações do Spark SQL
            Dataset<Row> cityStatisticsDF = processedDF
                    .groupBy("clean_city")
                    .agg(
                            count("*").alias("transaction_count"),
                            sum("clean_amount").alias("total_amount"),
                            avg("clean_amount").alias("average_amount")
                    )
                    // Ordenar por número de transações (decrescente)
                    .orderBy(col("transaction_count").desc());

            // Mostrar preview dos resultados
            System.out.println();
            System.out.println("Preview dos resultados (top 20 cidades):");
            System.out.println("========================================");
            cityStatisticsDF.show(20, false);

            // Calcular estatísticas globais
            System.out.println("Calculando estatísticas globais...");
            Row globalStats = cityStatisticsDF.agg(
                    count("*").alias("total_cities"),
                    sum("transaction_count").alias("total_transactions"),
                    sum("total_amount").alias("global_total_amount"),
                    avg("average_amount").alias("global_avg_amount")
            ).first();

            long totalCities = globalStats.getLong(0);
            long totalTransactions = globalStats.getLong(1);
            double globalTotalAmount = globalStats.getDouble(2);
            double globalAvgAmount = globalStats.getDouble(3);

            System.out.println();
            System.out.println("========================================");
            System.out.println("Estatísticas Globais:");
            System.out.println("  Total de cidades: " + totalCities);
            System.out.println("  Total de transações: " + totalTransactions);
            System.out.println("  Valor total geral: " + String.format("$%.2f", globalTotalAmount));
            System.out.println("  Ticket médio geral: " + String.format("$%.2f", globalAvgAmount));
            System.out.println();

            if (totalCities > 0) {
                long avgTransactionsPerCity = totalTransactions / totalCities;
                double avgAmountPerCity = globalTotalAmount / totalCities;
                System.out.println("  Médias por cidade:");
                System.out.println("    Transações por cidade: " + avgTransactionsPerCity);
                System.out.println("    Valor médio por cidade: " + String.format("$%.2f", avgAmountPerCity));
            }
            System.out.println("========================================");
            System.out.println();

            // Rankings: cidades com maiores/menores métricas
            System.out.println("Rankings:");
            System.out.println("----------------------------------------");

            // Cidade com mais transações
            Row topByCount = cityStatisticsDF
                    .orderBy(col("transaction_count").desc())
                    .first();
            System.out.println("Cidade com MAIS transações:");
            System.out.println("  " + topByCount.getString(0) + ": " +
                    topByCount.getLong(1) + " transações");
            System.out.println();

            // Cidade com maior volume financeiro
            Row topByTotal = cityStatisticsDF
                    .orderBy(col("total_amount").desc())
                    .first();
            System.out.println("Cidade com MAIOR volume financeiro:");
            System.out.println("  " + topByTotal.getString(0) + ": " +
                    String.format("$%.2f", topByTotal.getDouble(2)));
            System.out.println();

            // Cidade com maior ticket médio (minimo 10 transações)
            Dataset<Row> citiesWithMinTransactions = cityStatisticsDF
                    .filter(col("transaction_count").geq(10));

            if (citiesWithMinTransactions.count() > 0) {
                Row topByAverage = citiesWithMinTransactions
                        .orderBy(col("average_amount").desc())
                        .first();
                System.out.println("Cidade com MAIOR ticket médio (mín. 10 transações):");
                System.out.println("  " + topByAverage.getString(0) + ": " +
                        String.format("$%.2f", topByAverage.getDouble(3)));
                System.out.println();

                Row bottomByAverage = citiesWithMinTransactions
                        .orderBy(col("average_amount").asc())
                        .first();
                System.out.println("Cidade com MENOR ticket médio (mín. 10 transações):");
                System.out.println("  " + bottomByAverage.getString(0) + ": " +
                        String.format("$%.2f", bottomByAverage.getDouble(3)));
                System.out.println();
            }
            System.out.println("========================================");
            System.out.println();

            // Salvar resultados
            System.out.println("Salvando resultados em: " + outputPath);

            // Formatar output para ficar similar ao Hadoop
            Dataset<Row> formattedOutput = cityStatisticsDF
                    .withColumn("output",
                            concat(
                                    col("clean_city"),
                                    lit("\t"),
                                    lit("Transações: "),
                                    col("transaction_count"),
                                    lit(" | Total: $"),
                                    format_number(col("total_amount"), 2),
                                    lit(" | Média: $"),
                                    format_number(col("average_amount"), 2)
                            ))
                    .select("output");

            // Salvar como texto
            formattedOutput
                    .coalesce(1)  // Um único arquivo de saída
                    .write()
                    .mode("overwrite")
                    .text(outputPath);

            long endTime = System.currentTimeMillis();
            long executionTime = (endTime - startTime) / 1000;

            System.out.println();
            System.out.println("========================================");
            System.out.println("Job concluído com sucesso!");
            System.out.println("Tempo de execução: " + executionTime + " segundos");
            System.out.println("========================================");
            System.out.println();
            System.out.println("Para ver os resultados:");
            System.out.println("  cat " + outputPath + "/part-*.txt");

        } catch (Exception e) {
            System.err.println("Erro durante execução do job:");
            e.printStackTrace();
            System.exit(1);
        } finally {
            // Fechar SparkSession
            spark.stop();
        }
    }
}