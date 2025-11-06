package sparksql.routines.intermediate.topcategoriesbystate;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import sparksql.utils.MCCDescriptionMapper;
import java.util.Arrays;
import java.util.List;
import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/intermediate/top_categories_by_state

/**
 * Rotina intermediária que identifica as top 3 categorias (MCC) mais frequentes por estado dos EUA.
 * Usa Spark SQL (DataFrames/Datasets) para processar os dados.
 *
 * Filtra APENAS transações domésticas dos EUA (50 estados + DC), excluindo transações internacionais.
 *
 * Calcula para cada estado:
 * - As 3 categorias de produtos/serviços mais frequentes (baseado em códigos MCC)
 * - Contagem de transações para cada categoria
 * - Descrição legível de cada categoria
 */
public class TopCategoriesByState {

    // Estados válidos dos EUA (50 estados + DC) que serão ACEITOS
    private static final List<String> VALID_US_STATES = Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    );

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByState <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessão Spark
        SparkSession spark = SparkSession.builder()
                .appName("TopCategoriesByState")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Registra UDF (User Defined Function) para buscar a descrição do MCC
        spark.udf().register("getMCCDescription",
                (String mccCode) -> MCCDescriptionMapper.getDescription(mccCode),
                DataTypes.StringType);

        System.out.println("Iniciando TopCategoriesByState com Spark SQL\n");

        // Lê o arquivo CSV
        Dataset<Row> transactions = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(inputPath);

        // Mantém apenas transações de estados válidos dos EUA
        Dataset<Row> filteredData = transactions
                .withColumn("state", upper(trim(col("merchant_state"))))
                .withColumn("mcc", trim(col("mcc")))
                .filter(
                        col("state").isNotNull()
                                .and(col("mcc").isNotNull())
                                .and(not(col("state").isin("", "NULL", "N/A")))
                                .and(not(col("mcc").isin("", "NULL", "N/A")))
                                // ACEITA apenas se for um estado dos EUA
                                .and(col("state").isin(VALID_US_STATES.toArray()))
                );

        // Passo 1: Conta ocorrências de (estado, mcc)
        Dataset<Row> stateMccCounts = filteredData
                .groupBy("state", "mcc")
                .count()
                .withColumnRenamed("count", "mcc_count");

        // Passo 2: Define Janelas de Partição
        // Janela para ranking (Top 3 por estado)
        WindowSpec windowRank = Window
                .partitionBy("state")
                .orderBy(desc("mcc_count"));

        // Janela para total (Total de transações por estado)
        WindowSpec windowTotal = Window
                .partitionBy("state");

        // Passo 3: Calcula Ranks (Top N) e Totais
        Dataset<Row> rankedCategories = stateMccCounts
                .withColumn("rn", row_number().over(windowRank)) // Rank de cada MCC
                .withColumn("total_transactions", sum("mcc_count").over(windowTotal)) // Total do estado
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
                .groupBy("state", "total_transactions")
                .agg(
                        // Concatena as strings formatadas, separadas por " | "
                        array_join(collect_list("formatted_string"), " | ").as("TopCategories")
                )
                // Ordena o resultado final pelo total de transações
                .orderBy(desc("total_transactions"));


        // Mostra os 20 primeiros resultados
        System.out.println("Top 20 Estados (por volume de transações):");
        finalResults
                .select(col("state"), col("total_transactions"), col("TopCategories"))
                .show(20, false); // 'false' para não truncar a string

        // Calcula estatísticas globais
        long totalStates = finalResults.count();
        long totalUniqueMCCs = filteredData
                .select("mcc")
                .distinct()
                .count();

        System.out.println("\nEstatísticas Globais (Estados dos EUA):");
        System.out.println("  Total de estados: " + totalStates);
        System.out.println("  Total de categorias (MCC) únicas: " + totalUniqueMCCs);

        // Salva os resultados em formato CSV
        finalResults
                .select(col("state"), col("TopCategories"))
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Format: STATE,Top-1: MCC (Description) Count | Top-2: ... | Top-3: ...");

        spark.stop();
    }
}