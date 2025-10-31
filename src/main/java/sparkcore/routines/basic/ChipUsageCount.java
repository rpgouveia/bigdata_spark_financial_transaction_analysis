package sparkcore.routines.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_core/basic/chip_usage_count

/**
 * Rotina básica que conta os tipos de transação (chip vs swipe).
 * Usa Spark Core (RDDs) para processar os dados.
 */
public class ChipUsageCount {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ChipUsageCount <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuração do Spark
        SparkConf conf = new SparkConf()
                .setAppName("ChipUsageCount")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Lê o arquivo CSV como RDD
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabeçalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Mapeia para pares (tipo_transacao, 1) e conta por tipo
        JavaPairRDD<String, Integer> transactionTypeCounts = data
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String useChipRaw = fields[5];  // use_chip
                    String transactionType = processTransactionType(useChipRaw);
                    return new Tuple2<>(transactionType, 1);
                })
                .reduceByKey((a, b) -> a + b);

        // Ordena por contagem (decrescente)
        JavaPairRDD<String, Integer> sortedResults = transactionTypeCounts
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        // Mostra os resultados
        sortedResults.collect().forEach(tuple ->
                System.out.println(tuple._1 + "," + tuple._2)
        );

        // Salva os resultados
        sortedResults
                .map(tuple -> tuple._1 + "," + tuple._2)
                .coalesce(1)
                .saveAsTextFile(outputPath);

        System.out.println("Results saved to: " + outputPath);

        sc.stop();
    }


    // Processa e classifica o tipo de transação baseado no campo use_chip
    private static String processTransactionType(String useChipRaw) {
        if (useChipRaw == null || useChipRaw.trim().isEmpty()) {
            return "Unknown Transaction";
        }

        String useChip = useChipRaw.trim()
                .replace("\"", "")  // Remove aspas
                .toUpperCase();     // Padroniza em maiúsculas

        // Se já está em formato descritivo, manter
        if (useChip.contains("TRANSACTION")) {
            return capitalizeWords(useChip);
        }

        // Mapear valores simples para descrições
        switch (useChip) {
            case "Y":
            case "YES":
            case "TRUE":
            case "1":
                return "Chip Transaction";

            case "N":
            case "NO":
            case "FALSE":
            case "0":
                return "Swipe Transaction";

            case "ONLINE":
                return "Online Transaction";

            case "CONTACTLESS":
                return "Contactless Transaction";

            case "NULL":
            case "N/A":
            case "":
                return "Unknown Transaction";

            default:
                // Se não reconhecido, usar valor original capitalizado
                return capitalizeWords(useChip + " Transaction");
        }
    }

    // Capitaliza palavras para formatação consistente
    private static String capitalizeWords(String input) {
        if (input == null || input.isEmpty()) {
            return input;
        }

        StringBuilder result = new StringBuilder();
        String[] words = input.toLowerCase().split("\\s+");

        for (int i = 0; i < words.length; i++) {
            if (i > 0) {
                result.append(" ");
            }
            if (!words[i].isEmpty()) {
                result.append(Character.toUpperCase(words[i].charAt(0)));
                if (words[i].length() > 1) {
                    result.append(words[i].substring(1));
                }
            }
        }

        return result.toString();
    }

    // Split de CSV que respeita aspas e trata campos com vírgulas
    private static String[] splitCsv(String line) {
        List<String> result = new ArrayList<>();
        StringBuilder currentField = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);

            if (ch == '\"') {
                inQuotes = !inQuotes;
            } else if (ch == ',' && !inQuotes) {
                result.add(currentField.toString());
                currentField.setLength(0);
            } else {
                currentField.append(ch);
            }
        }

        // Adicionar último campo
        result.add(currentField.toString());

        return result.toArray(new String[0]);
    }
}