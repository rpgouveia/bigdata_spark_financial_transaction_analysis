package sparksql.routines.intermediate;

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
// src/main/resources/transactions_data.csv output/spark_sql/top_categories_by_city local

/**
 * TopCategoriesByCity usando Apache Spark SQL (DataFrames + Window Functions).
 *
 * Demonstra o uso de Window Functions do Spark SQL para ranqueamento de categorias
 * dentro de partições (cidades).
 *
 * Esta rotina identifica para cada cidade:
 * - As 3 categorias de produtos/serviços mais frequentes (baseado em MCC codes)
 * - Contagem de transações para cada categoria
 * - Descrição legível de cada categoria
 *
 * Usa MCCDescriptionMapper como classe utilitária para descrições de MCC.
 */
public class TopCategoriesByCity {

    public static void main(String[] args) {
        System.out.println("========================================");
        System.out.println("Iniciando TopCategoriesByCity com Spark SQL...");
        System.out.println("Rotina Intermediária - Window Functions");
        System.out.println("========================================");
        System.out.println();
        System.out.println("Objetivo: Identificar as top 3 categorias por cidade");
        System.out.println("  - Baseado em códigos MCC (Merchant Category Code)");
        System.out.println("  - Demonstra Window Functions para ranking");
        System.out.println("  - Descrições legíveis de categorias");
        System.out.println();
        System.out.println("Classe utilitária: MCCDescriptionMapper");
        System.out.println("  - Total de códigos MCC conhecidos: " + MCCDescriptionMapper.getTotalMCCCodes());
        System.out.println("  - Reutilizável em outras rotinas");
        System.out.println();

        // Verificação dos argumentos
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByCitySQL <input_path> <output_path> [local]");
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
                .appName("TopCategoriesByCity-SparkSQL");

        if (localMode) {
            System.out.println("Configurando para execução local...");
            sparkBuilder.master("local[*]");
        }

        SparkSession spark = sparkBuilder.getOrCreate();

        // Configurar nível de log
        spark.sparkContext().setLogLevel("WARN");

        // Registrar UDF para obter descrição do MCC usando a classe utilitária
        spark.udf().register("getMCCDescription",
                (String mccCode) -> MCCDescriptionMapper.getDescription(mccCode),
                DataTypes.StringType);

        try {
            System.out.println("========================================");
            System.out.println("Configuração do Job:");
            System.out.println("  Mode: " + (localMode ? "Local" : "Cluster"));
            System.out.println("  Input: " + inputPath);
            System.out.println("  Output: " + outputPath);
            System.out.println("  Engine: Spark SQL (Window Functions)");
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

            // Ler CSV
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

            // Processar dados: limpar cidade e MCC
            System.out.println("Processando e limpando dados...");
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
                    // Filtrar registros válidos
                    .filter(col("clean_mcc").notEqual("UNKNOWN_MCC")
                            .and(col("clean_mcc").rlike("\\d+")));  // MCC deve ser numérico

            // Passo 1: Agregar contagens por (cidade, mcc)
            System.out.println("Agregando transações por cidade e categoria...");
            Dataset<Row> cityMccCounts = processedDF
                    .groupBy("clean_city", "clean_mcc")
                    .agg(count("*").alias("transaction_count"))
                    .orderBy(col("clean_city"), col("transaction_count").desc());

            // Passo 2: Usar Window Function para rankear as categorias dentro de cada cidade
            System.out.println("Aplicando ranking com Window Functions...");

            WindowSpec windowSpec = Window
                    .partitionBy("clean_city")                  // Particionar por cidade
                    .orderBy(col("transaction_count").desc());  // Ordenar por contagem decrescente

            Dataset<Row> rankedCategories = cityMccCounts
                    .withColumn("rank", row_number().over(windowSpec))  // Adicionar coluna de ranking
                    .filter(col("rank").leq(3));                  // Filtrar apenas top 3

            // Passo 3: Adicionar descrição do MCC usando UDF
            Dataset<Row> categoriesWithDescription = rankedCategories
                    .withColumn("mcc_description",
                            callUDF("getMCCDescription", col("clean_mcc")));

            // Mostrar preview dos resultados
            System.out.println();
            System.out.println("Preview dos resultados (primeiras 30 linhas):");
            System.out.println("========================================");
            categoriesWithDescription
                    .select("clean_city", "rank", "clean_mcc", "mcc_description", "transaction_count")
                    .show(30, false);

            // Calcular estatísticas globais
            System.out.println("Calculando estatísticas globais...");

            // Total de cidades
            long totalCities = cityMccCounts.select("clean_city").distinct().count();

            // Total de categorias únicas
            long totalCategories = cityMccCounts.count();

            // Diversidade: contagem de categorias únicas por cidade
            Dataset<Row> diversityDF = cityMccCounts
                    .groupBy("clean_city")
                    .agg(count("*").alias("unique_categories"))
                    .orderBy(col("unique_categories").desc());

            Row mostDiverseCity = diversityDF.first();
            String cityWithMostDiversity = mostDiverseCity.getString(0);
            long highestUniqueMCCCount = mostDiverseCity.getLong(1);

            Dataset<Row> leastDiverseDF = diversityDF.orderBy(col("unique_categories").asc());
            Row leastDiverseCity = leastDiverseDF.first();
            String cityWithLeastDiversity = leastDiverseCity.getString(0);
            long lowestUniqueMCCCount = leastDiverseCity.getLong(1);

            // Média de categorias por cidade
            double avgCategoriesPerCity = (double) totalCategories / totalCities;

            System.out.println();
            System.out.println("========================================");
            System.out.println("Estatísticas Globais:");
            System.out.println("  Total de cidades: " + totalCities);
            System.out.println("  Total de pares (cidade, categoria): " + totalCategories);
            System.out.println("  Média de categorias por cidade: " + String.format("%.2f", avgCategoriesPerCity));
            System.out.println();
            System.out.println("  Diversidade de Mercado:");
            System.out.println("    Cidade com maior diversidade:");
            System.out.println("      " + cityWithMostDiversity + ": " + highestUniqueMCCCount + " categorias diferentes");
            System.out.println("    Cidade com menor diversidade:");
            System.out.println("      " + cityWithLeastDiversity + ": " + lowestUniqueMCCCount + " categorias diferentes");
            System.out.println();

            System.out.println("  Insights:");
            if (avgCategoriesPerCity > 20) {
                System.out.println("    Alta diversidade comercial no dataset");
            } else if (avgCategoriesPerCity > 10) {
                System.out.println("    Diversidade comercial moderada");
            } else {
                System.out.println("    Baixa diversidade comercial (poucas categorias)");
            }
            System.out.println("========================================");
            System.out.println();

            // Exemplos de cidades com suas top 3 categorias
            System.out.println("Exemplos de análises (5 cidades aleatórias):");
            System.out.println("----------------------------------------");
            categoriesWithDescription
                    .orderBy(rand())
                    .limit(15)  // 5 cidades × 3 categorias = 15 linhas
                    .show(15, false);

            // Salvar resultados
            System.out.println("Salvando resultados em: " + outputPath);

            // Formatar output para ficar similar ao Hadoop
            // Formato: CIDADE    Top-1: MCC (Descrição) Count | Top-2: ... | Top-3: ...
            Dataset<Row> formattedOutput = categoriesWithDescription
                    .withColumn("formatted_category",
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
                    .groupBy("clean_city")
                    .agg(
                            collect_list("formatted_category").alias("categories_list")
                    )
                    .withColumn("output",
                            concat(
                                    col("clean_city"),
                                    lit("    "),
                                    concat_ws(" | ", col("categories_list"))
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
            System.out.println();
            System.out.println("Formato do output:");
            System.out.println("  CIDADE    Top-1: MCC (Descrição) Count | Top-2: ... | Top-3: ...");
            System.out.println();

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