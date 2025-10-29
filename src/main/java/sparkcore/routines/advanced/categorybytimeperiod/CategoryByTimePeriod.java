package sparkcore.routines.advanced.categorybytimeperiod;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_core/advanced/category_by_time_period

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import sparkcore.routines.intermediate.topcategoriesbycity.CategoryCount;
import sparkcore.routines.intermediate.topcategoriesbycity.MCCDescriptionMapper;

import scala.Tuple2;

import java.util.*;

/**
 * Rotina avancada que identifica as top 3 categorias (MCC) por periodo do dia e cidade.
 * Usa Spark Core (RDDs) para processar os dados.
 *
 * Calcula para cada combinacao de cidade + periodo:
 * - As 3 categorias de produtos/servicos mais frequentes (baseado em codigos MCC)
 * - Contagem de transacoes para cada categoria
 * - Descricao legivel de cada categoria
 *
 * Periodos:
 * - MORNING (Manha): 00:00 - 11:59
 * - AFTERNOON (Tarde): 12:00 - 17:59
 * - NIGHT (Noite): 18:00 - 23:59
 */
public class CategoryByTimePeriod {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: CategoryByTimePeriod <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuracao do Spark
        SparkConf conf = new SparkConf()
                .setAppName("CategoryByTimePeriod")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("========================================");
        System.out.println("Iniciando CategoryByTimePeriod com Spark RDDs");
        System.out.println("Analise Multi-dimensional: Cidade + Periodo + Categoria");
        System.out.println("========================================");

        long startTime = System.currentTimeMillis();

        // Le o arquivo CSV como RDD
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabecalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Passo 1: Mapeia para ((CityPeriodKey, mcc), 1) e conta ocorrencias
        JavaPairRDD<Tuple2<CityPeriodKey, String>, Long> cityPeriodMccCounts = data
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String city = fields[7].trim().toUpperCase();  // merchant_city
                    String dateTime = fields[1].trim();  // date
                    String mcc = fields[10].trim();  // mcc code

                    // Determina o periodo do dia
                    String period = determineTimePeriod(dateTime);

                    // Cria chave composta
                    CityPeriodKey key = new CityPeriodKey(city, period);

                    return new Tuple2<>(new Tuple2<>(key, mcc), 1L);
                })
                .filter(tuple -> !tuple._1._1.getTimePeriod().equals("UNKNOWN"))
                .reduceByKey((a, b) -> a + b);

        // Passo 2: Transforma para (CityPeriodKey, CategoryCount)
        JavaPairRDD<CityPeriodKey, CategoryCount> cityPeriodCategories = cityPeriodMccCounts
                .mapToPair(tuple -> {
                    CityPeriodKey key = tuple._1._1;
                    String mcc = tuple._1._2;
                    long count = tuple._2;
                    return new Tuple2<>(key, new CategoryCount(mcc, count));
                });

        // Passo 3: Agrupa por CityPeriodKey e pega top 3
        JavaPairRDD<CityPeriodKey, String> topCategoriesByPeriod = cityPeriodCategories
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

        // Passo 4: Calcula total de transacoes por CityPeriodKey para ordenacao
        JavaPairRDD<CityPeriodKey, Long> cityPeriodTotals = cityPeriodMccCounts
                .mapToPair(tuple -> new Tuple2<>(tuple._1._1, tuple._2))
                .reduceByKey((a, b) -> a + b);

        // Passo 5: Join e ordenar
        JavaPairRDD<CityPeriodKey, Tuple2<String, Long>> sortedResults = topCategoriesByPeriod
                .join(cityPeriodTotals)
                .mapToPair(tuple -> new Tuple2<>(tuple._2._2, new Tuple2<>(tuple._1, tuple._2._1)))
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<>(tuple._2._1, new Tuple2<>(tuple._2._2, tuple._1)));

        long endTime = System.currentTimeMillis();

        // Mostra os 30 primeiros resultados
        System.out.println("\n========================================");
        System.out.println("Top 30 Cidade-Periodos (por volume de transacoes):");
        System.out.println("========================================");

        List<Tuple2<CityPeriodKey, Tuple2<String, Long>>> top30 = sortedResults.take(30);

        for (Tuple2<CityPeriodKey, Tuple2<String, Long>> tuple : top30) {
            CityPeriodKey key = tuple._1;
            String categories = tuple._2._1;
            long total = tuple._2._2;
            System.out.println(key.toDisplayString() + " (Total: " + total + " transacoes)");
            System.out.println("  " + categories);
        }

        // Calcula estatisticas globais
        long totalCityPeriods = topCategoriesByPeriod.count();

        // Estatisticas por periodo
        Map<String, Long> transactionsByPeriod = cityPeriodTotals
                .mapToPair(tuple -> new Tuple2<>(tuple._1.getTimePeriod(), tuple._2))
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();

        Map<String, Long> cityCountByPeriod = topCategoriesByPeriod
                .mapToPair(tuple -> new Tuple2<>(tuple._1.getTimePeriod(), 1L))
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();

        long totalTransactions = transactionsByPeriod.values().stream()
                .mapToLong(Long::longValue).sum();

        System.out.println("\n========================================");
        System.out.println("Estatisticas Globais:");
        System.out.println("  Total de cidade-periodos: " + totalCityPeriods);
        System.out.println("  Total de transacoes: " + totalTransactions);
        System.out.println();
        System.out.println("  Resumo por Periodo:");

        String[] periods = {"MORNING", "AFTERNOON", "NIGHT"};
        String[] periodNames = {"Manha (0h-11h)", "Tarde (12h-17h)", "Noite (18h-23h)"};

        for (int i = 0; i < periods.length; i++) {
            String period = periods[i];
            String periodName = periodNames[i];
            long periodTxns = transactionsByPeriod.getOrDefault(period, 0L);
            long cityCount = cityCountByPeriod.getOrDefault(period, 0L);

            if (periodTxns > 0) {
                double pct = (periodTxns * 100.0) / totalTransactions;
                System.out.println(String.format("    %s: %d transacoes (%.2f%%) em %d cidades",
                        periodName, periodTxns, pct, cityCount));
            }
        }

        System.out.println();
        System.out.println("  Tempo de processamento: " + (endTime - startTime) + "ms");
        System.out.println("========================================");

        // Salva os resultados em formato CSV
        sortedResults
                .map(tuple -> tuple._1.toDisplayString() + "," + tuple._2._1)
                .coalesce(1)
                .saveAsTextFile(outputPath);

        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Format: CITY [PERIOD],Top-1: MCC (Description) Count | Top-2: ... | Top-3: ...");

        sc.stop();
    }

    /**
     * Determina o periodo do dia baseado no timestamp
     */
    private static String determineTimePeriod(String dateTimeRaw) {
        if (dateTimeRaw == null || dateTimeRaw.trim().isEmpty()) {
            return "UNKNOWN";
        }

        try {
            String dateTime = dateTimeRaw.trim().replace("\"", "");
            String[] parts = dateTime.split(" ");
            if (parts.length < 2) {
                return "UNKNOWN";
            }

            String timePart = parts[1];
            String[] timeParts = timePart.split(":");
            if (timeParts.length < 1) {
                return "UNKNOWN";
            }

            int hour = Integer.parseInt(timeParts[0]);

            if (hour >= 0 && hour < 12) {
                return "MORNING";
            } else if (hour >= 12 && hour < 18) {
                return "AFTERNOON";
            } else if (hour >= 18 && hour < 24) {
                return "NIGHT";
            }

            return "UNKNOWN";

        } catch (Exception e) {
            return "UNKNOWN";
        }
    }
}