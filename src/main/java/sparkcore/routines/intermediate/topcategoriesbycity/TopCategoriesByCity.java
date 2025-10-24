package sparkcore.routines.intermediate.topcategoriesbycity;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/intermediate/top_categories_by_city

/**
 * Rotina intermediária que identifica as top 3 categorias (MCC) mais frequentes por cidade.
 * Usa Spark Core (RDDs) para processar os dados.
 *
 * Calcula para cada cidade:
 * - As 3 categorias de produtos/serviços mais frequentes (baseado em códigos MCC)
 * - Contagem de transações para cada categoria
 * - Descrição legível de cada categoria
 */
public class TopCategoriesByCity {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: TopCategoriesByCity <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuração do Spark
        SparkConf conf = new SparkConf()
                .setAppName("TopCategoriesByCity")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("Iniciando TopCategoriesByCity com Spark RDDs\n");

        // Lê o arquivo CSV como RDD
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabeçalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Passo 1: Mapeia para ((cidade, mcc), 1) e conta ocorrências
        JavaPairRDD<Tuple2<String, String>, Long> cityMccCounts = data
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String city = fields[7].trim().toUpperCase();  // merchant_city
                    String mcc = fields[10].trim();  // mcc code
                    return new Tuple2<>(new Tuple2<>(city, mcc), 1L);
                })
                .reduceByKey((a, b) -> a + b);

        // Passo 2: Transforma para (cidade, CategoryCount)
        JavaPairRDD<String, CategoryCount> cityCategories = cityMccCounts
                .mapToPair(tuple -> {
                    String city = tuple._1._1;
                    String mcc = tuple._1._2;
                    long count = tuple._2;
                    return new Tuple2<>(city, new CategoryCount(mcc, count));
                });

        // Passo 3: Agrupa por cidade e pega top 3
        JavaPairRDD<String, String> topCategoriesByCity = cityCategories
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

        // Passo 4: Calcula total de transações por cidade para ordenação
        JavaPairRDD<String, Long> cityTotalCounts = cityMccCounts
                .mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2))
                .reduceByKey((a, b) -> a + b);

        // Passo 5: Join para adicionar total e ordenar
        JavaPairRDD<String, Tuple2<String, Long>> sortedResults = topCategoriesByCity
                .join(cityTotalCounts)  // Resultado: (city, (categories, total))
                .mapToPair(tuple -> new Tuple2<>(tuple._2._2, new Tuple2<>(tuple._1, tuple._2._1)))  // (total, (city, categories))
                .sortByKey(false)  // Ordena por total decrescente
                .mapToPair(tuple -> new Tuple2<>(tuple._2._1, new Tuple2<>(tuple._2._2, tuple._1)));  // (city, (categories, total))

        // Mostra os 20 primeiros resultados
        System.out.println("Top 20 Cidades (por volume de transações):");

        sortedResults.take(20).forEach(tuple -> {
            String city = tuple._1;
            String categories = tuple._2._1;
            long total = tuple._2._2;
            System.out.println(city + " (Total: " + total + " transações)");
            System.out.println("  " + categories);
        });

        // Calcula estatísticas globais
        long totalCities = topCategoriesByCity.count();
        long totalUniqueMCCs = cityMccCounts
                .map(tuple -> tuple._1._2)
                .distinct()
                .count();

        System.out.println("\nEstatísticas Globais:");
        System.out.println("  Total de cidades: " + totalCities);
        System.out.println("  Total de categorias (MCC) únicas: " + totalUniqueMCCs);

        // Salva os resultados em formato CSV
        sortedResults
                .map(tuple -> tuple._1 + "," + tuple._2._1)
                .coalesce(1)
                .saveAsTextFile(outputPath);

        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Format: CITY,Top-1: MCC (Description) Count | Top-2: ... | Top-3: ...");

        sc.stop();
    }
}