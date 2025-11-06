package sparksql.routines.intermediate.topcategoriesbycountry;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import java.util.Arrays;
import java.util.List;
import sparksql.utils.MCCDescriptionMapper;
import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/intermediate/top_categories_by_country

/**
 * Rotina intermediária que identifica as top 3 categorias (MCC) mais frequentes por país.
 * Usa Spark SQL (DataFrames/Datasets) para processar os dados.
 *
 * Filtra APENAS transações internacionais, excluindo os 50 estados dos EUA + DC.
 *
 * Calcula para cada país:
 * - As 3 categorias de produtos/serviços mais frequentes (baseado em códigos MCC)
 * - Contagem de transações para cada categoria
 * - Descrição legível de cada categoria
 */
public class TopCategoriesByCountry {

    // Lista de estados dos EUA que serão REJEITADOS
    private static final List<String> US_STATES_TO_REJECT = Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    );

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByCountry <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessão Spark
        SparkSession spark = SparkSession.builder()
                .appName("TopCategoriesByCountry")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Registra UDF (User Defined Function) para buscar a descrição do MCC
        spark.udf().register("getMCCDescription",
                (String mccCode) -> MCCDescriptionMapper.getDescription(mccCode),
                DataTypes.StringType);

        System.out.println("Iniciando TopCategoriesByCountry com Spark SQL\n");

        // Lê o arquivo CSV
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        // Remove transações dos EUA e linhas malformadas
        Dataset<Row> filteredData = transactions
                .withColumn("country", upper(trim(col("merchant_state"))))
                .withColumn("mcc", trim(col("mcc")))
                .filter(
                        col("country").isNotNull()
                                .and(col("mcc").isNotNull())
                                .and(not(col("country").isin("", "NULL", "N/A")))
                                .and(not(col("mcc").isin("", "NULL", "N/A")))
                                // REJEITA se for um estado dos EUA
                                .and(not(col("country").isin(US_STATES_TO_REJECT.toArray())))
                );

        // Passo 1: Conta ocorrências de (pais, mcc)
        Dataset<Row> countryMccCounts = filteredData
                .groupBy("country", "mcc")
                .count()
                .withColumnRenamed("count", "mcc_count");

        // Passo 2: Define Janelas de Partição
        // Janela para ranking (Top 3 por país)
        WindowSpec windowRank = Window
                .partitionBy("country")
                .orderBy(desc("mcc_count"));

        // Janela para total (Total de transações por país)
        WindowSpec windowTotal = Window
                .partitionBy("country");

        // Passo 3: Calcula Ranks (Top N) e Totais
        Dataset<Row> rankedCategories = countryMccCounts
                .withColumn("rn", row_number().over(windowRank)) // Rank de cada MCC
                .withColumn("total_transactions", sum("mcc_count").over(windowTotal)) // Total do país
                .withColumn("mcc_description", call_udf("getMCCDescription", col("mcc"))); // Busca descrição

        // Passo 4: Filtra o Top 3 e formata a string de saída
        Dataset<Row> top3Formatted = rankedCategories
                .filter(col("rn").leq(3))
                .withColumn("formatted_string", format_string(
                        "Top-%d: %s (%s) %d",
                        col("rn"),
                        col("mcc"),
                        col("mcc_description"),
                        col("mcc_count")
                ))
                .orderBy(col("rn"));

        // Passo 5: Agrega as strings do Top 3 em uma única coluna
        Dataset<Row> finalResults = top3Formatted
                .groupBy("country", "total_transactions")
                .agg(
                        // Concatena as strings formatadas, separadas por " | "
                        array_join(collect_list("formatted_string"), " | ").as("TopCategories")
                )
                // Ordena o resultado final pelo total de transações
                .orderBy(desc("total_transactions"));


        // Mostra os 20 primeiros resultados
        System.out.println("Top 20 Países (por volume de transações internacionais):");
        finalResults
                .select(col("country"), col("total_transactions"), col("TopCategories"))
                .show(20, false);

        // Calcula estatísticas globais
        long totalCountries = finalResults.count();
        long totalUniqueMCCs = filteredData
                .select("mcc")
                .distinct()
                .count();

        System.out.println("\nEstatísticas Globais (Internacional):");
        System.out.println("  Total de países: " + totalCountries);
        System.out.println("  Total de categorias (MCC) únicas: " + totalUniqueMCCs);

        // Salva os resultados em formato CSV
        finalResults
                .select(col("country"), col("TopCategories"))
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Format: COUNTRY,Top-1: MCC (Description) Count | Top-2: ... | Top-3: ...");

        spark.stop();
    }
}