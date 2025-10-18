package routines.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.math.BigDecimal;
import java.math.RoundingMode;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/basic/amount_by_client

/**
 * Rotina básica que calcula o valor total transacionado por cliente.
 * Usa Spark Core (RDDs) para processar os dados.
 */
public class AmountByClient {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: AmountByClient <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuração do Spark
        SparkConf conf = new SparkConf()
                .setAppName("AmountByClient")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // Lê o arquivo CSV como RDD
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabeçalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Mapeia para pares (cliente, valor) e soma por cliente
        JavaPairRDD<String, Double> clientAmounts = data
                .mapToPair(line -> {
                    String[] fields = line.split(",");
                    String client = fields[2];  // client_id
                    String amountStr = fields[4].replace("$", "");  // amount
                    Double amount = Double.parseDouble(amountStr);
                    return new Tuple2<>(client, amount);
                })
                .reduceByKey((a, b) -> a + b);

        // Arredonda para 2 casas decimais
        JavaPairRDD<String, Double> roundedAmounts = clientAmounts
                .mapValues(amount -> {
                    BigDecimal bd = new BigDecimal(amount);
                    bd = bd.setScale(2, RoundingMode.HALF_UP);
                    return bd.doubleValue();
                });

        // Ordena por valor (decrescente)
        JavaPairRDD<String, Double> sortedResults = roundedAmounts
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1))
                .sortByKey(false)
                .mapToPair(tuple -> new Tuple2<>(tuple._2, tuple._1));

        // Mostra os 20 primeiros resultados
        sortedResults.take(20).forEach(tuple ->
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
}