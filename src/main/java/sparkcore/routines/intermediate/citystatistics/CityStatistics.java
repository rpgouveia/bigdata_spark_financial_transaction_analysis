package sparkcore.routines.intermediate.citystatistics;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.math.BigDecimal;
import java.math.RoundingMode;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/intermediate/city_statistics

/**
 * Rotina intermediária que calcula estatísticas completas por cidade.
 * Usa Spark Core (RDDs) para processar os dados.
 *
 * Calcula para cada cidade:
 * - Número total de transações
 * - Valor total transacionado
 * - Valor médio por transação (ticket médio)
 */
public class CityStatistics {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: CityStatistics <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuração do Spark
        SparkConf conf = new SparkConf()
                .setAppName("CityStatistics")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("Iniciando CityStatistics com Spark RDDs\n");

        // Lê o arquivo CSV como RDD
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabeçalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Mapeia para pares (cidade, CityStats) com uma transação cada
        JavaPairRDD<String, CityStats> cityStats = data
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String city = fields[7].trim().toUpperCase();  // merchant_city
                    String amountStr = fields[4].replace("$", "");  // amount

                    // Converte valor para centavos
                    long amountInCents = parseAmountToCents(amountStr);

                    // Cria CityStats com uma transação
                    CityStats stats = new CityStats(1, amountInCents);

                    return new Tuple2<>(city, stats);
                })
                .filter(tuple -> tuple._2.getTotalAmountInCents() != Long.MIN_VALUE);

        // Agrega as estatísticas por cidade usando reduceByKey
        JavaPairRDD<String, CityStats> aggregatedStats = cityStats
                .reduceByKey((stats1, stats2) -> stats1.add(stats2));

        // Ordena por número de transações (decrescente)
        JavaPairRDD<String, CityStats> sortedResults = aggregatedStats
                .mapToPair(tuple -> new Tuple2<>(tuple._2.getTransactionCount(), tuple))
                .sortByKey(false)
                .mapToPair(tuple -> tuple._2);

        // Mostra os 20 primeiros resultados
        System.out.println("Top 20 Cidades por Número de Transações:");

        sortedResults.take(20).forEach(tuple -> {
            String city = tuple._1;
            CityStats stats = tuple._2;
            System.out.println(city + " -> " + stats.toString());
        });

        // Calcula e mostra estatísticas globais
        long totalCities = aggregatedStats.count();
        long totalTransactions = aggregatedStats
                .map(tuple -> tuple._2.getTransactionCount())
                .reduce((a, b) -> a + b);

        long totalAmountInCents = aggregatedStats
                .map(tuple -> tuple._2.getTotalAmountInCents())
                .reduce((a, b) -> a + b);

        System.out.println("\nEstatísticas Globais:");
        System.out.println("  Total de cidades: " + totalCities);
        System.out.println("  Total de transações: " + totalTransactions);
        System.out.println("  Valor total: $" + String.format("%.2f", totalAmountInCents / 100.0));
        System.out.println("  Ticket médio global: $" + String.format("%.2f", (totalAmountInCents / 100.0) / totalTransactions));

        // Salva os resultados em formato CSV
        sortedResults
                .map(tuple -> tuple._1 + "," + tuple._2.toCSV())
                .coalesce(1)
                .saveAsTextFile(outputPath);

        System.out.println("\nResults saved to: " + outputPath);
        System.out.println("Format: CITY,TransactionCount,TotalAmount,AverageAmount");

        sc.stop();
    }

    /**
     * Converte string de valor monetário para centavos (long)
     * Formato esperado: $14.57 (formato americano)
     */
    private static long parseAmountToCents(String rawAmount) {
        if (rawAmount == null || rawAmount.trim().isEmpty()) {
            return Long.MIN_VALUE;
        }

        try {
            String cleanAmount = rawAmount.trim()
                    .replace("\"", "")
                    .replace("$", "")
                    .replace(" ", "")
                    .replace(",", "");  // Remove separadores de milhares

            if (cleanAmount.isEmpty()) {
                return Long.MIN_VALUE;
            }

            // Converter para BigDecimal para precisão
            BigDecimal amount = new BigDecimal(cleanAmount);

            // Converter para centavos (multiplicar por 100)
            BigDecimal amountInCents = amount.movePointRight(2);

            // Arredondar e converter para long
            return amountInCents.setScale(0, RoundingMode.HALF_UP).longValueExact();

        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }
}