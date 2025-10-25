package sparkcore.routines.intermediate.topcategoriesbycountry;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_core/intermediate/top_categories_by_country

/**
 * Rotina intermediária que identifica as top 3 categorias (MCC) mais frequentes por país.
 * Usa Spark Core (RDDs) para processar os dados.
 *
 * Filtra APENAS transações internacionais, excluindo os 50 estados dos EUA + DC.
 *
 * Calcula para cada país:
 * - As 3 categorias de produtos/serviços mais frequentes (baseado em códigos MCC)
 * - Contagem de transações para cada categoria
 * - Descrição legível de cada categoria
 */
public class TopCategoriesByCountry {

    // Conjunto de estados dos EUA que serão REJEITADOS
    private static final Set<String> US_STATES_TO_REJECT = new HashSet<>(Arrays.asList(
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
    ));

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByCountry <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuração do Spark
        SparkConf conf = new SparkConf()
                .setAppName("TopCategoriesByCountry")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("Iniciando TopCategoriesByCountry com Spark RDDs\n");

        // Lê o arquivo CSV como RDD
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabeçalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Filtro: Remove transações dos EUA e linhas malformadas
        JavaRDD<String> filteredData = data.filter(line -> {
            try {
                String[] fields = line.split(",");
                if (fields.length < 11) return false; // Garante que temos os campos 8 e 10
                String location = fields[8].trim().toUpperCase(); // merchant_state (país)
                String mcc = fields[10].trim();

                // Validação de dados
                if (mcc.isEmpty() || mcc.equals("NULL") || mcc.equals("N/A")) return false;
                if (location.isEmpty() || location.equals("NULL") || location.equals("N/A")) return false;

                // Lógica principal: REJEITA se for um estado dos EUA
                return !US_STATES_TO_REJECT.contains(location);

            } catch (Exception e) {
                return false; // Descarta linhas com erro de parsing
            }
        });

        // Passo 1: Mapeia para ((pais, mcc), 1) e conta ocorrências
        JavaPairRDD<Tuple2<String, String>, Long> countryMccCounts = filteredData
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String country = fields[8].trim().toUpperCase();  // merchant_state (país)
                    String mcc = fields[10].trim();  // mcc code
                    return new Tuple2<>(new Tuple2<>(country, mcc), 1L);
                })
                .reduceByKey((a, b) -> a + b);

        // Passo 2: Transforma para (pais, CategoryCount)
        JavaPairRDD<String, CategoryCount> countryCategories = countryMccCounts
                .mapToPair(tuple -> {
                    String country = tuple._1._1;
                    String mcc = tuple._1._2;
                    long count = tuple._2;
                    return new Tuple2<>(country, new CategoryCount(mcc, count));
                });

        // Passo 3: Agrupa por pais e pega top 3
        JavaPairRDD<String, String> topCategoriesByCountry = countryCategories
                .groupByKey()
                .mapValues(categories -> {
                    // Converte Iterable para List e ordena
                    List<CategoryCount> categoryList = new ArrayList<>();
                    categories.forEach(categoryList::add);

                    // Ordena por count (decrescente)
                    Collections.sort(categoryList);

                    // Pega top 3
                    int topN = Math.min(3, categoryList.size());
                    StringBuilder result = new StringBuilder();

                    for (int i = 0; i < topN; i++) {
                        if (i > 0) {
                            result.append(" | ");
                        }
                        result.append(categoryList.get(i).toFormattedString(i + 1));
                    }

                    return result.toString();
                });

        // Passo 4: Calcula total de transações por pais para ordenação
        JavaPairRDD<String, Long> countryTotalCounts = countryMccCounts
                .mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2))
                .reduceByKey((a, b) -> a + b);

        // Passo 5: Join para adicionar total e ordenar
        JavaPairRDD<String, Tuple2<String, Long>> sortedResults = topCategoriesByCountry
                .join(countryTotalCounts)  // Resultado: (pais, (categories, total))
                .mapToPair(tuple -> new Tuple2<>(tuple._2._2, new Tuple2<>(tuple._1, tuple._2._1)))  // (total, (pais, categories))
                .sortByKey(false)  // Ordena por total decrescente
                .mapToPair(tuple -> new Tuple2<>(tuple._2._1, new Tuple2<>(tuple._2._2, tuple._1)));  // (pais, (categories, total))

        // Mostra os 20 primeiros resultados
        System.out.println("Top 20 Países (por volume de transações internacionais):");

        sortedResults.take(20).forEach(tuple -> {
            String country = tuple._1;
            String categories = tuple._2._1;
            long total = tuple._2._2;
            System.out.println(country + " (Total: " + total + " transações)");
            System.out.println("  " + categories);
        });

        // Calcula estatísticas globais
        long totalCountries = topCategoriesByCountry.count();
        long totalUniqueMCCs = countryMccCounts
                .map(tuple -> tuple._1._2)
                .distinct()
                .count();

        System.out.println("\nEstatísticas Globais (Internacional):");
        System.out.println("  Total de países: " + totalCountries);
        System.out.println("  Total de categorias (MCC) únicas: " + totalUniqueMCCs);

        // Salva os resultados em formato CSV
        sortedResults
                .map(tuple -> tuple._1 + "," + tuple._2._1)
                .coalesce(1)
                .saveAsTextFile(outputPath);

        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Format: COUNTRY,Top-1: MCC (Description) Count | Top-2: ... | Top-3: ...");

        sc.stop();
    }
}