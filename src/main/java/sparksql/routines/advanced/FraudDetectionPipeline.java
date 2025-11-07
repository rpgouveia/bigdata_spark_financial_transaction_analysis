package sparksql.routines.advanced;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/advanced/fraud_detection_pipeline local

/**
 * FraudDetectionPipeline usando Apache Spark SQL
 *
 * Esta rotina implementa análise de risco de fraudes:
 * - Perfil comportamental de cada cliente (13 métricas)
 * - Risk scoring com 7 fatores ponderados
 * - Classificação em categorias (LOW, MEDIUM, HIGH, CRITICAL)
 * - Relatórios consolidados com top 10 por categoria
 *
 * Comparação de arquiteturas:
 * HADOOP MAPREDUCE: 3 Jobs Encadeados
 * - Job 1 → Client Profile Builder (agregação comportamental)
 * - Job 2 → Risk Category Classifier (risk scoring)
 * - Job 3 → Final Risk Report Generator (relatórios consolidados)
 *
 * SPARK CORE: 5 Etapas com RDDs
 * - Etapa 1 → Agregação (groupByKey para construir perfis por cliente)
 * - Etapa 2 → Classificação (mapValues para calcular risk score)
 * - Etapa 3 → Contagem (reduceByKey para totais por categoria)
 * - Etapa 4 → Agrupamento (groupByKey para clientes por categoria)
 * - Etapa 5 → Ordenação (sortByKey para ordenar por severidade)
 *
 * SPARK SQL: Pipeline Único
 * - DataFrame transformations → Agregação → Risk Scoring → Classificação → Relatórios
 */
public class FraudDetectionPipeline {

    public static void main(String[] args) {
        System.out.println("============================================================");
        System.out.println("    FRAUD DETECTION PIPELINE - SPARK SQL");
        System.out.println("============================================================");
        System.out.println();
        System.out.println("Objetivo: Análise de risco de fraudes baseada em comportamento");
        System.out.println();
        System.out.println("Métricas Comportamentais:");
        System.out.println("  - Mobilidade (cidades diferentes)");
        System.out.println("  - Diversidade de categorias (MCCs)");
        System.out.println("  - Múltiplos cartões");
        System.out.println("  - Taxa de erros");
        System.out.println("  - Chargebacks (estornos)");
        System.out.println("  - Valor médio de transações");
        System.out.println("  - Proporção online vs presencial");
        System.out.println();
        System.out.println("Categorias de Risco:");
        System.out.println("  - LOW (0-30 pontos): Comportamento normal");
        System.out.println("  - MEDIUM (31-60 pontos): Alguns sinais de alerta");
        System.out.println("  - HIGH (61-85 pontos): Múltiplos indicadores de risco");
        System.out.println("  - CRITICAL (86-100+ pontos): Risco extremo");
        System.out.println();

        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: RiskAnalysisPipelineSQL <input_path> <output_path> [local]");
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
                .appName("RiskAnalysisPipeline-SparkSQL");

        if (localMode) {
            System.out.println("Configurando para execução local...");
            sparkBuilder.master("local[*]");
        }

        SparkSession spark = sparkBuilder.getOrCreate();

        // Configurar nível de log
        spark.sparkContext().setLogLevel("WARN");

        // Registrar UDFs
        registerUDFs(spark);

        try {
            System.out.println("============================================================");
            System.out.println("Configuração do Pipeline:");
            System.out.println("  Mode: " + (localMode ? "Local" : "Cluster"));
            System.out.println("  Input: " + inputPath);
            System.out.println("  Output: " + outputPath);
            System.out.println("  Engine: Spark SQL (Pipeline Único)");
            System.out.println("============================================================");
            System.out.println();

            long totalStartTime = System.currentTimeMillis();

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

            // STEP 1: LER E PROCESSAR TRANSAÇÕES
            System.out.println(">>> STEP 1: Lendo e processando transações...");
            long step1Start = System.currentTimeMillis();

            Dataset<Row> transactionsDF = spark.read()
                    .option("header", "true")
                    .option("quote", "\"")
                    .option("escape", "\"")
                    .schema(schema)
                    .csv(inputPath);

            long totalTransactions = transactionsDF.count();
            System.out.println("Total de transações lidas: " + totalTransactions);

            // Processar campos
            Dataset<Row> processedDF = transactionsDF
                    // Converter timestamp
                    .withColumn("timestamp",
                            to_timestamp(regexp_replace(col("date"), "\"", ""), "yyyy-MM-dd HH:mm:ss"))
                    // Limpar amount (remover $ e converter)
                    .withColumn("clean_amount",
                            regexp_replace(col("amount"), "\\$", "").cast("double"))
                    // Limpar cidade
                    .withColumn("clean_city",
                            when(col("merchant_city").isNull()
                                            .or(trim(col("merchant_city")).equalTo(""))
                                            .or(upper(trim(col("merchant_city"))).equalTo("ONLINE")),
                                    lit("ONLINE"))
                                    .otherwise(upper(trim(regexp_replace(col("merchant_city"), "\"", "")))))
                    // Limpar MCC
                    .withColumn("clean_mcc",
                            trim(regexp_replace(col("mcc"), "\"", "")))
                    // Limpar card_id
                    .withColumn("clean_card",
                            trim(regexp_replace(col("card_id"), "\"", "")))
                    // Detectar transação online
                    .withColumn("is_online",
                            when(col("use_chip").contains("Online"), 1).otherwise(0))
                    // Detectar erro
                    .withColumn("has_error",
                            when(col("errors").isNotNull()
                                            .and(trim(col("errors")).notEqual(""))
                                            .and(upper(trim(col("errors"))).notEqual("NULL"))
                                            .and(upper(trim(col("errors"))).notEqual("N/A")),
                                    1)
                                    .otherwise(0))
                    // Detectar chargeback (valor negativo)
                    .withColumn("is_chargeback",
                            when(col("clean_amount").lt(0), 1).otherwise(0))
                    // Filtrar registros válidos
                    .filter(col("client_id").isNotNull()
                            .and(col("timestamp").isNotNull())
                            .and(col("clean_amount").isNotNull()));

            long validTransactions = processedDF.count();
            System.out.println("Transações válidas: " + validTransactions);

            long step1Duration = System.currentTimeMillis() - step1Start;
            System.out.println(">>> STEP 1 COMPLETED in " + (step1Duration / 1000) + " seconds");
            System.out.println();

            // STEP 2: CONSTRUIR PERFIL COMPORTAMENTAL POR CLIENTE
            System.out.println(">>> STEP 2: Building Client Profiles...");
            long step2Start = System.currentTimeMillis();

            Dataset<Row> clientProfilesDF = processedDF
                    .groupBy("client_id")
                    .agg(
                            // Contagem de transações
                            count("*").alias("transaction_count"),
                            // Valor total (usa abs para considerar chargebacks como positivo)
                            sum(abs(col("clean_amount"))).alias("total_amount"),
                            // Valor médio
                            avg(abs(col("clean_amount"))).alias("avg_amount"),
                            // Cidades únicas (excluindo ONLINE)
                            countDistinct(
                                    when(col("clean_city").notEqual("ONLINE"), col("clean_city"))
                            ).alias("unique_cities"),
                            // MCCs únicos
                            countDistinct(col("clean_mcc")).alias("unique_mccs"),
                            // Cartões únicos
                            countDistinct(col("clean_card")).alias("unique_cards"),
                            // Primeira transação
                            min(col("timestamp")).cast("long").alias("first_transaction"),
                            // Última transação
                            max(col("timestamp")).cast("long").alias("last_transaction"),
                            // Contagem online
                            sum(col("is_online")).alias("online_count"),
                            // Contagem swipe (presencial)
                            sum(when(col("is_online").equalTo(0), 1).otherwise(0)).alias("swipe_count"),
                            // Contagem de erros
                            sum(col("has_error")).alias("error_count"),
                            // Contagem de chargebacks
                            sum(col("is_chargeback")).alias("chargeback_count")
                    )
                    .cache();  // Cache porque usaremos múltiplas vezes

            long totalClients = clientProfilesDF.count();
            System.out.println("Total de clientes: " + totalClients);

            System.out.println();
            System.out.println("Amostra de perfis (primeiros 10 clientes):");
            clientProfilesDF
                    .select("client_id", "transaction_count", "total_amount",
                            "unique_cities", "unique_mccs", "error_count")
                    .orderBy(col("total_amount").desc())
                    .show(10, false);

            long step2Duration = System.currentTimeMillis() - step2Start;
            System.out.println(">>> STEP 2 COMPLETED in " + (step2Duration / 1000) + " seconds");
            System.out.println();

            // STEP 3: CALCULAR RISK SCORE E CLASSIFICAR
            System.out.println(">>> STEP 3: Classifying Risk Categories...");
            long step3Start = System.currentTimeMillis();

            // Calcular risk score usando UDF
            Dataset<Row> withRiskScoreDF = clientProfilesDF
                    .withColumn("risk_result",
                            callUDF("calculateRiskScore",
                                    col("transaction_count"),
                                    col("unique_cities"),
                                    col("unique_mccs"),
                                    col("unique_cards"),
                                    col("error_count"),
                                    col("chargeback_count"),
                                    col("avg_amount"),
                                    col("online_count")))
                    // Extrair risk score e factors do resultado
                    .withColumn("risk_score",
                            element_at(split(col("risk_result"), "\\|"), 1).cast("double"))
                    .withColumn("risk_factors",
                            element_at(split(col("risk_result"), "\\|"), 2))
                    // Classificar em categoria
                    .withColumn("risk_category",
                            when(col("risk_score").geq(86), "CRITICAL")
                                    .when(col("risk_score").geq(61), "HIGH")
                                    .when(col("risk_score").geq(31), "MEDIUM")
                                    .otherwise("LOW"))
                    .drop("risk_result")
                    .cache();

            // Estatísticas por categoria
            System.out.println();
            System.out.println("Distribuição por categoria de risco:");
            withRiskScoreDF
                    .groupBy("risk_category")
                    .agg(
                            count("*").alias("client_count"),
                            sum("total_amount").alias("total_amount"),
                            avg("risk_score").alias("avg_risk_score")
                    )
                    .orderBy(
                            when(col("risk_category").equalTo("CRITICAL"), 4)
                                    .when(col("risk_category").equalTo("HIGH"), 3)
                                    .when(col("risk_category").equalTo("MEDIUM"), 2)
                                    .otherwise(1).desc()
                    )
                    .show(false);

            long step3Duration = System.currentTimeMillis() - step3Start;
            System.out.println(">>> STEP 3 COMPLETED in " + (step3Duration / 1000) + " seconds");
            System.out.println();

            // STEP 4: GERAR RELATÓRIOS FINAIS POR CATEGORIA
            System.out.println(">>> STEP 4: Generating Final Risk Reports...");
            long step4Start = System.currentTimeMillis();

            // Window Function para ranking dentro de cada categoria
            WindowSpec categoryWindow = Window
                    .partitionBy("risk_category")
                    .orderBy(col("risk_score").desc());

            // Adicionar ranking
            Dataset<Row> rankedDF = withRiskScoreDF
                    .withColumn("rank_in_category", row_number().over(categoryWindow))
                    .cache();

            // Gerar relatório consolidado por categoria
            System.out.println();
            System.out.println("============================================================");
            System.out.println("                 RELATÓRIOS POR CATEGORIA");
            System.out.println("============================================================");

            // Para cada categoria, gerar relatório
            String[] categories = {"CRITICAL", "HIGH", "MEDIUM", "LOW"};

            StringBuilder fullReport = new StringBuilder();

            for (String category : categories) {
                Dataset<Row> categoryDF = rankedDF.filter(col("risk_category").equalTo(category));

                long categoryCount = categoryDF.count();
                if (categoryCount == 0) continue;

                // Estatísticas da categoria
                Row stats = categoryDF
                        .agg(
                                count("*").alias("total_clients"),
                                avg("risk_score").alias("avg_risk_score"),
                                sum("total_amount").alias("total_amount"),
                                avg("total_amount").alias("avg_amount_per_client"),
                                avg("transaction_count").alias("avg_transactions")
                        )
                        .first();

                long totalClientsInCategory = stats.getLong(0);
                double avgRiskScore = stats.getDouble(1);
                double totalAmount = stats.getDouble(2);
                double avgAmountPerClient = stats.getDouble(3);
                double avgTransactions = stats.getDouble(4);

                // Construir relatório
                StringBuilder report = new StringBuilder();
                report.append("\n========== RISK CATEGORY: ").append(category).append(" ==========\n");
                report.append(String.format(Locale.US, "Total Clients: %d\n", totalClientsInCategory));
                report.append(String.format(Locale.US, "Average Risk Score: %.2f\n", avgRiskScore));
                report.append(String.format(Locale.US, "Total Amount: $%.2f\n", totalAmount));
                report.append(String.format(Locale.US, "Average Amount per Client: $%.2f\n", avgAmountPerClient));
                report.append(String.format(Locale.US, "Average Transactions per Client: %.1f\n", avgTransactions));
                report.append("\n");

                // Top 10 clientes
                report.append("--- TOP 10 HIGHEST RISK CLIENTS ---\n");
                List<Row> top10 = categoryDF
                        .filter(col("rank_in_category").leq(10))
                        .orderBy("rank_in_category")
                        .collectAsList();

                for (Row client : top10) {
                    int rank = client.getInt(client.fieldIndex("rank_in_category"));
                    String clientId = client.getString(client.fieldIndex("client_id"));
                    double riskScore = client.getDouble(client.fieldIndex("risk_score"));
                    int txnCount = Math.toIntExact(client.getLong(client.fieldIndex("transaction_count")));
                    double amount = client.getDouble(client.fieldIndex("total_amount"));
                    String factors = client.getString(client.fieldIndex("risk_factors"));

                    report.append(String.format(Locale.US,
                            "%d. Client %s (Score: %.2f, Transactions: %d, Amount: $%.2f)\n" +
                                    "   Factors: %s\n",
                            rank, clientId, riskScore, txnCount, amount, factors));
                }

                report.append("========================================\n");

                // Imprimir no console
                System.out.println(report.toString());

                // Adicionar ao relatório completo
                fullReport.append(report.toString());
            }

            long step4Duration = System.currentTimeMillis() - step4Start;
            System.out.println(">>> STEP 4 COMPLETED in " + (step4Duration / 1000) + " seconds");
            System.out.println();

            // SALVAR RESULTADOS
            System.out.println("Salvando resultados em: " + outputPath);

            // Salvar relatório consolidado
            List<String> reportList = Collections.singletonList(fullReport.toString());
            Dataset<String> reportDS = spark.createDataset(reportList, Encoders.STRING());
            Dataset<Row> reportDF = reportDS.toDF("report");

            reportDF
                    .coalesce(1)
                    .write()
                    .mode("overwrite")
                    .text(outputPath + "/risk_report");

            // Salvar detalhes de todos os clientes classificados
            rankedDF
                    .select("risk_category", "client_id", "risk_score", "risk_factors",
                            "transaction_count", "total_amount", "unique_cities", "unique_mccs",
                            "unique_cards", "error_count", "chargeback_count", "rank_in_category")
                    .orderBy(
                            when(col("risk_category").equalTo("CRITICAL"), 4)
                                    .when(col("risk_category").equalTo("HIGH"), 3)
                                    .when(col("risk_category").equalTo("MEDIUM"), 2)
                                    .otherwise(1).desc(),
                            col("risk_score").desc()
                    )
                    .coalesce(1)
                    .write()
                    .mode("overwrite")
                    .option("header", "true")
                    .csv(outputPath + "/client_classifications");

            // ESTATÍSTICAS FINAIS
            long totalDuration = System.currentTimeMillis() - totalStartTime;

            System.out.println();
            System.out.println("============================================================");
            System.out.println("       PIPELINE EXECUTION SUMMARY");
            System.out.println("============================================================");
            System.out.println("Step 1 Duration: " + (step1Duration / 1000) + " seconds (Processar Transações)");
            System.out.println("Step 2 Duration: " + (step2Duration / 1000) + " seconds (Perfis Comportamentais)");
            System.out.println("Step 3 Duration: " + (step3Duration / 1000) + " seconds (Risk Scoring)");
            System.out.println("Step 4 Duration: " + (step4Duration / 1000) + " seconds (Relatórios Finais)");
            System.out.println("------------------------------------------------------------");
            System.out.println("Total Duration: " + (totalDuration / 1000) + " seconds");
            System.out.println("============================================================");
            System.out.println();
            System.out.println("Outputs gerados:");
            System.out.println("  1. " + outputPath + "/risk_report (Relatório consolidado)");
            System.out.println("  2. " + outputPath + "/client_classifications (Detalhes por cliente)");
            System.out.println();
            System.out.println("============================================================");

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
     * Registra UDFs necessárias para o pipeline
     */
    private static void registerUDFs(SparkSession spark) {
        // UDF para calcular risk score com 8 parâmetros
        // Usa função lambda simples que delega para o método calculateRiskScoreFull
        spark.udf().register("calculateRiskScore",
                (Long transactionCount, Long uniqueCities, Long uniqueMccs, Long uniqueCards,
                 Long errorCount, Long chargebackCount, Double avgAmount, Long onlineCount) -> {
                    return calculateRiskScoreFull(
                            transactionCount.intValue(),
                            uniqueCities.intValue(),
                            uniqueMccs.intValue(),
                            uniqueCards.intValue(),
                            errorCount.intValue(),
                            chargebackCount.intValue(),
                            avgAmount,
                            onlineCount.intValue()
                    );
                },
                DataTypes.StringType);
    }

    /**
     * Calcula risk score baseado em múltiplos fatores comportamentais
     * Retorna: "score|factors"
     */
    private static String calculateRiskScoreFull(int transactionCount, int uniqueCities,
                                                 int uniqueMccs, int uniqueCards,
                                                 int errorCount, int chargebackCount,
                                                 double avgAmount, int onlineCount) {
        double riskScore = 0.0;
        List<String> riskFactors = new ArrayList<>();

        // Fator 1: Mobilidade (cidades diferentes)
        if (uniqueCities > 5) {
            riskScore += 15;
            riskFactors.add("HIGH_MOBILITY[" + uniqueCities + "_cities]");
        } else if (uniqueCities > 3) {
            riskScore += 8;
            riskFactors.add("MEDIUM_MOBILITY[" + uniqueCities + "_cities]");
        }

        // Fator 2: Diversidade de categorias MCC
        if (uniqueMccs > 10) {
            riskScore += 12;
            riskFactors.add("DIVERSE_MCC[" + uniqueMccs + "_categories]");
        } else if (uniqueMccs > 6) {
            riskScore += 6;
            riskFactors.add("VARIED_MCC[" + uniqueMccs + "_categories]");
        }

        // Fator 3: Múltiplos cartões
        if (uniqueCards > 3) {
            riskScore += 20;
            riskFactors.add("MULTIPLE_CARDS[" + uniqueCards + "_cards]");
        } else if (uniqueCards > 1) {
            riskScore += 8;
            riskFactors.add("DUAL_CARDS[" + uniqueCards + "_cards]");
        }

        // Fator 4: Taxa de erros
        if (transactionCount > 0) {
            double errorRate = (errorCount * 100.0) / transactionCount;
            if (errorRate > 20) {
                riskScore += 25;
                riskFactors.add(String.format(Locale.US, "HIGH_ERROR_RATE[%.1f%%]", errorRate));
            } else if (errorRate > 10) {
                riskScore += 12;
                riskFactors.add(String.format(Locale.US, "MEDIUM_ERROR_RATE[%.1f%%]", errorRate));
            }
        }

        // Fator 5: Chargebacks
        if (chargebackCount > 3) {
            riskScore += 25;
            riskFactors.add("FREQUENT_CHARGEBACKS[" + chargebackCount + "]");
        } else if (chargebackCount > 0) {
            riskScore += 10;
            riskFactors.add("CHARGEBACKS[" + chargebackCount + "]");
        }

        // Fator 6: Valor médio alto
        if (avgAmount > 500) {
            riskScore += 15;
            riskFactors.add(String.format(Locale.US, "HIGH_AVG_AMOUNT[%.2f]", avgAmount));
        } else if (avgAmount > 200) {
            riskScore += 7;
            riskFactors.add(String.format(Locale.US, "MEDIUM_AVG_AMOUNT[%.2f]", avgAmount));
        }

        // Fator 7: Proporção online vs presencial
        if (transactionCount > 0) {
            double onlineRate = (onlineCount * 100.0) / transactionCount;
            if (onlineRate > 80 || onlineRate < 20) {
                riskScore += 10;
                riskFactors.add(String.format(Locale.US, "UNBALANCED_CHANNELS[%.0f%%_online]", onlineRate));
            }
        }

        // Se não há fatores de risco, é comportamento normal
        String factorsStr = riskFactors.isEmpty() ? "NORMAL_BEHAVIOR" : String.join("; ", riskFactors);

        return String.format(Locale.US, "%.2f|%s", riskScore, factorsStr);
    }
}