package sparksql.routines.advanced;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import sparksql.utils.MCCDescriptionMapper;
import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/advanced/category_by_timeperiod local

/**
 * CategoryByTimePeriod usando Apache Spark SQL
 *
 * Esta rotina identifica padrões de consumo por horário:
 * - Classifica transações em períodos (Manhã/Tarde/Noite)
 * - Top 3 categorias mais frequentes por cidade e período
 * - Análise multidimensional de comportamento temporal
 *
 * Comparação de arquiteturas:
 * HADOOP MAPREDUCE: 2 Jobs Encadeados
 * - Job 1 → Agregação (cidade + período + MCC)
 * - Job 2 → Ranking (top 3 por cidade-período)
 *
 * SPARK CORE: 4 Etapas com RDDs
 * - Etapa 1 → Agregação (reduceByKey para contar MCCs)
 * - Etapa 2 → Ranking (groupByKey + ordenação para top 3)
 * - Etapa 3 → Totais (reduceByKey para volumes)
 * - Etapa 4 → Join e ordenação final (join + sortByKey)
 *
 * SPARK SQL: Pipeline Único
 * - DataFrame transformations → Agregação → Window Functions → Ranking
 */
public class CategoryByTimePeriod {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Iniciando CategoryByTimePeriod com Spark SQL...");
        System.out.println("Rotina Avançada - Pipeline Único");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Objetivo: Identificar padrões de consumo por horário");
        System.out.println("  - Top 3 categorias por período e cidade");
        System.out.println("  - Períodos: Manhã (0h-11h), Tarde (12h-17h), Noite (18h-23h)");
        System.out.println();
        System.out.println("Arquitetura:");
        System.out.println("  Hadoop: 2 jobs encadeados (Agregação + Ranking)");
        System.out.println("  Spark SQL: 1 pipeline único");
        System.out.println();

        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: CategoryByTimePeriodSQL <input_path> <output_path> [local]");
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
                .appName("CategoryByTimePeriod-SparkSQL");

        if (localMode) {
            System.out.println("Configurando para execução local...");
            sparkBuilder.master("local[*]");
        }

        SparkSession spark = sparkBuilder.getOrCreate();

        // Configurar nível de log
        spark.sparkContext().setLogLevel("WARN");

        // Registrar UDF para descrição de MCC
        spark.udf().register("getMCCDescription",
                (String mccCode) -> MCCDescriptionMapper.getDescription(mccCode),
                DataTypes.StringType);

        try {
            System.out.println("========================================");
            System.out.println("Configuração do Job:");
            System.out.println("  Mode: " + (localMode ? "Local" : "Cluster"));
            System.out.println("  Input: " + inputPath);
            System.out.println("  Output: " + outputPath);
            System.out.println("  Engine: Spark SQL (Pipeline Único)");
            System.out.println("========================================");
            System.out.println();

            long startTime = System.currentTimeMillis();

            // Definir schema do CSV
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

            // ETAPA 1: Ler e processar dados
            System.out.println("Etapa 1: Lendo e processando CSV...");
            Dataset<Row> transactionsDF = spark.read()
                    .option("header", "true")
                    .option("quote", "\"")
                    .option("escape", "\"")
                    .schema(schema)
                    .csv(inputPath);

            long totalRecords = transactionsDF.count();
            System.out.println("Total de registros lidos: " + totalRecords);
            System.out.println();

            // ETAPA 2: Adicionar colunas derivadas
            System.out.println("Etapa 2: Processando cidades, MCCs e períodos...");
            Dataset<Row> processedDF = transactionsDF
                    // Limpar cidade
                    .withColumn("clean_city",
                            when(col("merchant_city").isNull()
                                            .or(trim(col("merchant_city")).equalTo(""))
                                            .or(upper(trim(col("merchant_city"))).equalTo("NULL"))
                                            .or(upper(trim(col("merchant_city"))).equalTo("N/A")),
                                    lit("UNKNOWN"))
                                    .otherwise(upper(trim(regexp_replace(col("merchant_city"), "\"", "")))))
                    // Limpar MCC
                    .withColumn("clean_mcc",
                            when(col("mcc").isNull()
                                            .or(trim(col("mcc")).equalTo(""))
                                            .or(upper(trim(col("mcc"))).equalTo("NULL"))
                                            .or(upper(trim(col("mcc"))).equalTo("N/A")),
                                    lit("UNKNOWN_MCC"))
                                    .otherwise(trim(regexp_replace(col("mcc"), "\"", ""))))
                    // Extrair timestamp (formato: "YYYY-MM-DD HH:MM:SS")
                    .withColumn("timestamp",
                            to_timestamp(regexp_replace(col("date"), "\"", ""), "yyyy-MM-dd HH:mm:ss"))
                    // Extrair hora do dia
                    .withColumn("hour", hour(col("timestamp")))
                    // Classificar em período do dia
                    .withColumn("time_period",
                            when(col("hour").between(0, 11), "MORNING")
                                    .when(col("hour").between(12, 17), "AFTERNOON")
                                    .otherwise("NIGHT"))
                    // Filtrar registros válidos
                    .filter(col("clean_mcc").notEqual("UNKNOWN_MCC")
                            .and(col("clean_mcc").rlike("\\d+"))
                            .and(col("timestamp").isNotNull()));

            long validRecords = processedDF.count();
            System.out.println("Registros válidos após processamento: " + validRecords);
            System.out.println();

            // Estatísticas de distribuição por período
            System.out.println("Distribuição por período:");
            processedDF.groupBy("time_period")
                    .agg(count("*").alias("count"))
                    .orderBy("time_period")
                    .show(false);

            // ETAPA 3: Agregação
            System.out.println("Etapa 3: Agregando transações por cidade-período-MCC...");
            Dataset<Row> aggregatedDF = processedDF
                    .groupBy("clean_city", "time_period", "clean_mcc")
                    .agg(count("*").alias("transaction_count"))
                    .cache();  // Cache porque usaremos múltiplas vezes

            long aggregatedCount = aggregatedDF.count();
            System.out.println("Total de combinações (cidade, período, MCC): " + aggregatedCount);
            System.out.println();

            // ETAPA 4: Ranking com Window Functions
            System.out.println("Etapa 4: Aplicando Window Functions para ranking...");

            // Window Function: particionar por (cidade, período) e ordenar por contagem
            WindowSpec windowSpec = Window
                    .partitionBy("clean_city", "time_period")
                    .orderBy(col("transaction_count").desc());

            // Aplicar ranking e filtrar top 3
            Dataset<Row> rankedDF = aggregatedDF
                    .withColumn("rank", row_number().over(windowSpec))
                    .filter(col("rank").leq(3))
                    .cache();

            long rankedCount = rankedDF.count();
            System.out.println("Total de registros no top 3: " + rankedCount);
            System.out.println();

            // ETAPA 5: Adicionar descrições de MCC
            System.out.println("Etapa 5: Adicionando descrições de categorias...");
            Dataset<Row> withDescriptionsDF = rankedDF
                    .withColumn("mcc_description",
                            callUDF("getMCCDescription", col("clean_mcc")))
                    // Adicionar nome legível do período
                    .withColumn("period_name",
                            when(col("time_period").equalTo("MORNING"), "Manhã")
                                    .when(col("time_period").equalTo("AFTERNOON"), "Tarde")
                                    .otherwise("Noite"));

            // Prévia dos resultados
            System.out.println("Prévia dos resultados (primeiras 30 linhas):");
            System.out.println("========================================");
            withDescriptionsDF
                    .select("clean_city", "period_name", "rank", "clean_mcc",
                            "mcc_description", "transaction_count")
                    .orderBy("clean_city", "time_period", "rank")
                    .show(30, false);

            // ETAPA 6: Estatísticas Globais
            System.out.println("Calculando estatísticas globais...");

            // Total de cidade-períodos únicos
            long uniqueCityPeriods = withDescriptionsDF
                    .select("clean_city", "time_period")
                    .distinct()
                    .count();

            // Estatísticas por período
            Dataset<Row> periodStats = processedDF
                    .groupBy("time_period")
                    .agg(
                            count("*").alias("total_transactions"),
                            countDistinct("clean_city").alias("unique_cities")
                    )
                    .withColumn("period_name",
                            when(col("time_period").equalTo("MORNING"), "Manhã")
                                    .when(col("time_period").equalTo("AFTERNOON"), "Tarde")
                                    .otherwise("Noite"));

            long totalTransactions = processedDF.count();

            System.out.println();
            System.out.println("========================================");
            System.out.println("Estatísticas Globais:");
            System.out.println("  Total de transações: " + totalTransactions);
            System.out.println("  Combinações únicas (cidade + período): " + uniqueCityPeriods);
            System.out.println();

            System.out.println("  Resumo por Período:");
            periodStats
                    .withColumn("percentage",
                            round(
                                    col("total_transactions")
                                            .cast("double")
                                            .divide(lit(totalTransactions))
                                            .multiply(lit(100)),
                                    2
                            ))
                    .select("period_name", "total_transactions", "percentage", "unique_cities")
                    .orderBy("time_period")
                    .show(false);

            // Cidade-período com maior diversidade
            Dataset<Row> diversity = aggregatedDF
                    .groupBy("clean_city", "time_period")
                    .agg(count("*").alias("unique_categories"))
                    .orderBy(col("unique_categories").desc());

            Row mostDiverse = diversity.first();
            String mostDiverseCity = mostDiverse.getString(0);
            String mostDiversePeriod = mostDiverse.getString(1);
            long mostDiverseCount = mostDiverse.getLong(2);

            System.out.println();
            System.out.println("  Maior diversidade de categorias:");
            System.out.println("    " + mostDiverseCity + " [" +
                    getPeriodName(mostDiversePeriod) + "]: " +
                    mostDiverseCount + " categorias diferentes");
            System.out.println("========================================");
            System.out.println();

            // ETAPA 7: Formatar output para salvar
            System.out.println("Salvando resultados em: " + outputPath);

            // Formatar cada linha como: "Top-N: MCC (Descrição) Count"
            Dataset<Row> formattedDF = withDescriptionsDF
                    .withColumn("category_info",
                            concat(
                                    lit("Top-"),
                                    col("rank"),
                                    lit(": "),
                                    col("clean_mcc"),
                                    lit(" ("),
                                    col("mcc_description"),
                                    lit(") "),
                                    col("transaction_count")
                            ))
                    // Agrupar por cidade-período e coletar as top 3
                    .groupBy("clean_city", "time_period", "period_name")
                    .agg(collect_list("category_info").alias("categories_list"))
                    // Criar output final
                    .withColumn("output",
                            concat(
                                    col("clean_city"),
                                    lit(" ["),
                                    col("period_name"),
                                    lit("]    "),
                                    concat_ws(" | ", col("categories_list"))
                            ))
                    .select("output")
                    .orderBy("output");

            // Salvar resultado
            formattedDF
                    .coalesce(1)
                    .write()
                    .mode("overwrite")
                    .text(outputPath);

            long endTime = System.currentTimeMillis();
            long executionTime = (endTime - startTime) / 1000;

            System.out.println();
            System.out.println("========================================");
            System.out.println("Pipeline concluído com sucesso!");
            System.out.println("Tempo de execução: " + executionTime + " segundos");
            System.out.println("========================================");
            System.out.println();
            System.out.println("Para ver os resultados:");
            System.out.println("  cat " + outputPath + "/part-*.txt");
            System.out.println();
            System.out.println("Formato do output:");
            System.out.println("  CIDADE [Período]    Top-1: MCC (Descrição) Count | Top-2: ... | Top-3: ...");
            System.out.println();
            System.out.println("Exemplo:");
            System.out.println("  NEW YORK [Manhã]    Top-1: 5812 (Restaurants) 850 | Top-2: 5411 (Supermarkets) 620 | ...");
            System.out.println();

        } catch (Exception e) {
            System.err.println("Erro durante execução do pipeline:");
            e.printStackTrace();
            System.exit(1);
        } finally {
            // Fechar SparkSession
            spark.stop();
        }
    }

    /**
     * Converte código do período para nome legível
     */
    private static String getPeriodName(String period) {
        switch (period) {
            case "MORNING":
                return "Manhã";
            case "AFTERNOON":
                return "Tarde";
            case "NIGHT":
                return "Noite";
            default:
                return period;
        }
    }
}