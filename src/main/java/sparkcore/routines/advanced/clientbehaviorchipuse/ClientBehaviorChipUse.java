package sparkcore.routines.advanced.clientbehaviorchipuse;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_core/advanced/client_behavior_chip_use

/**
 * Rotina avancada que analisa o comportamento do cliente baseado em transacoes.
 * Usa Spark Core (RDDs) para processar os dados.
 * Objetivo:
 * 1. Perfilar cada cliente (Job 1):
 * - Calcular metricas: onlineRate, errorRate, avgCents, maxCents.
 * - Determinar UF (Estado) predominante do cliente.
 * - Classificar o cliente em Risco (LOW, MED, HIGH).
 *
 * 2. Agregar por Estado (Job 2):
 * - Contar clientes por Risco (LOW, MED, HIGH) em cada UF.
 * - Identificar as Top 5 cidades com mais clientes de ALTO Risco em cada UF.
 *
 * 3. Ordenar:
 * - O resultado final e ordenado pela porcentagem de clientes de ALTO Risco (decrescente).
 */

public class ClientBehaviorChipUse {

    // Parametros de Risco
    private static final float RISK_ERROR_HIGH = 0.05f; // 5%
    private static final float RISK_ERROR_MED = 0.02f; // 2%
    //ONLINE
    private static final float RISK_ONLINE_HIGH = 0.80f;
    private static final float RISK_ONLINE_MED = 0.60f;
    // PRECO
    private static final long RISK_AVG_HIGH_CENTS = 10000L; // medio $100
    private static final long RISK_MAX_HIGH_CENTS = 50000L; // alto $500
    private static final int TOPN_CITIES = 5;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: ClientBehaviorChipUse <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria configuracao do Spark
        SparkConf conf = new SparkConf()
                .setAppName("ClientBehaviorChipUse")
                .setMaster("local[*]");

        // Cria contexto Spark
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("========================================");
        System.out.println("Iniciando ClientBehaviorChipUse");
        System.out.println("Perfil do cliente: online vs. falhas");
        System.out.println("========================================");

        long startTime = System.currentTimeMillis();

        // Le o arquivo CSV
        JavaRDD<String> lines = sc.textFile(inputPath);

        // Remove o cabecalho
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header));

        // Passo 1: Mapear CSV para (client_id, TransactionMini) - ClientAggMapper
        JavaPairRDD<String, TransactionMini> clientTransactions = data.mapToPair(line -> {
            String[] parts = splitCsv(line);
            if (parts.length < 12) {
                return new Tuple2<>(null, null); // Sera filtrado
            }

            // Colunas: id(0), date(1), client_id(2), card_id(3), amount(4), use_chip(5) [channel],
            // merchant_id(6), merchant_city(7), merchant_state(8), zip(9), mcc(10), errors(11)

            String clientId = trimQ(parts[2]);
            String amountRaw = trimQ(parts[4]);
            String channelRaw = trimQ(parts[5]).toUpperCase();
            String merchantCity = trimQ(parts[7]);
            String merchantState = trimQ(parts[8]);
            String mcc = trimQ(parts[10]);
            String errorsRaw = parts[11];

            if (clientId.isEmpty()) {
                return new Tuple2<>(null, null);
            }

            boolean isOnline = "ONLINE TRANSACTION".equals(channelRaw);
            long amountCents = parseAmountToCents(amountRaw);
            if (amountCents == Long.MIN_VALUE) {
                return new Tuple2<>(null, null);
            }

            boolean hasError = (errorsRaw != null && !errorsRaw.trim().isEmpty() && !errorsRaw.trim().equals("\"\""));

            TransactionMini mini = new TransactionMini(
                    isOnline, hasError, amountCents, merchantCity, merchantState, mcc
            );

            return new Tuple2<>(clientId, mini);

        }).filter(t -> t._1 != null);


        // Passo 2: Agrupar por client_id (Shuffle)
        JavaPairRDD<String, Iterable<TransactionMini>> groupedByClient = clientTransactions.groupByKey();

        // Passo 3: Perfilar Cliente e Mapear para (Estado, AgregadoCliente) - ClientAggReducer
        JavaPairRDD<String, StateClientAgg> stateAggregates = groupedByClient.mapToPair(tuple -> {
            String clientId = tuple._1;
            Iterable<TransactionMini> values = tuple._2;

            long tx = 0L;
            long onlineCount = 0L;
            long errors = 0L;
            long sumCents = 0L;
            long maxCents = 0L;

            Map<String, Long> stateCount = new HashMap<>();
            Map<String, Long> cityCount = new HashMap<>();

            for (TransactionMini v : values) {
                tx++;
                if (v.isOnline()) onlineCount++;
                if (v.isHasError()) errors++;
                long a = v.getAmountCents();
                sumCents += a;
                if (a > maxCents) maxCents = a;

                stateCount.merge(nz(v.getState()), 1L, Long::sum);
                cityCount.merge(nz(v.getCity()), 1L, Long::sum);
            }

            if (tx == 0) {
                return new Tuple2<>(null, null); // Filtra
            }

            String topState = topKey(stateCount);
            String topCity = topKey(cityCount);

            // Metricas do Cliente
            double onlineRate = onlineCount * 1.0 / tx;
            double errorRate = errors * 1.0 / tx;
            long avgCents = sumCents / tx;

            // Classificacao
            String bucket = classifyBucket(
                    onlineRate, errorRate, avgCents, maxCents,
                    RISK_ERROR_HIGH, RISK_ERROR_MED, RISK_ONLINE_HIGH, RISK_ONLINE_MED,
                    RISK_AVG_HIGH_CENTS, RISK_MAX_HIGH_CENTS
            );

            // Emitir (Estado, ObjetoAgregado) - StateAggMapper
            StateClientAgg agg = StateClientAgg.singleClient(bucket, topCity);

            return new Tuple2<>(topState, agg);

        }).filter(t -> t._1 != null && !t._1.equals("UNKNOWN")); // Filtra clientes sem estado


        // Passo 4: Agregar por Estado - StateAggCombiner/Reducer
        JavaPairRDD<String, StateClientAgg> finalStateAgg = stateAggregates
                .reduceByKey((agg1, agg2) -> agg1.add(agg2));

        // Cache da saida do Job 1
        finalStateAgg.cache();
        long job1Count = finalStateAgg.count();
        System.out.println("========================================");
        System.out.println("Agregacao por Cliente");
        System.out.println("Estados unicos processados: " + job1Count);
        System.out.println("========================================");


        // Passo 5: Calcular Top N Cidades e Formatar - StateAggReducer
        JavaPairRDD<StateSummary, String> finalSummaries = finalStateAgg.mapToPair(tuple -> {
            String state = tuple._1;
            StateClientAgg agg = tuple._2;

            // Ordena cidades por contagem de Alto Risco
            List<Map.Entry<String, Long>> topCities = new ArrayList<>(agg.getHighRiskCityCounts().entrySet());
            topCities.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));


            // Converter para Tuple2
            int actualTopN = Math.min(topCities.size(), TOPN_CITIES);

            List<Tuple2<String, Long>> serializableTopList = topCities.subList(0, actualTopN)
                    .stream()
                    .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList());

            StateSummary summary = new StateSummary(agg, serializableTopList, TOPN_CITIES);

            // Inverte (Sumario, Estado) para ordenacao
            return new Tuple2<>(summary, state);
        });


        // Passo 6: Ordena por % High Risk, definido no 'compareTo' de StateSummary)
        JavaRDD<String> formattedOutput = finalSummaries
                .sortByKey(false) // Descendente
                .map(tuple -> tuple._2 + "\t" + tuple._1.toString());


        // Salva o resultado
        formattedOutput.coalesce(1).saveAsTextFile(outputPath);

        long endTime = System.currentTimeMillis();
        System.out.println("========================================");
        System.out.println("Agregacao por Estado");
        System.out.println("Tempo total de processamento: " + (endTime - startTime) + "ms");
        System.out.println("Resultados salvos em: " + outputPath);
        System.out.println("========================================");

        sc.stop();
    }


    /**
     * Regras de classificacao de risco - ClientAggReducer
     */
    private static String classifyBucket(double onlineRate, double errorRate, long avgCents, long maxCents, float errHigh, float errMed, float onHigh, float onMed, long avgHigh, long maxHigh) {
        boolean highRisk =
                (errorRate >= errHigh && onlineRate >= onMed) ||
                        (onlineRate >= onHigh) ||
                        (avgCents >= avgHigh) ||
                        (maxCents >= maxHigh);

        if (highRisk) return "HIGH";

        boolean medium =
                (errorRate >= errMed) ||
                        (onlineRate >= onMed);

        return medium ? "MED" : "LOW";
    }

    /**
     * Retorna a chave com maior frequencia (do ClientAggReducer)
     */
    private static String topKey(Map<String, Long> map) {
        String best = "";
        long bestV = -1L;
        for (Map.Entry<String, Long> e : map.entrySet()) {
            if (e.getValue() > bestV) {
                bestV = e.getValue();
                best = e.getKey();
            }
        }
        return best;
    }

    /**
     * Normaliza strings nulas/vazias - ClientAggReducer
     */
    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s.trim().toUpperCase();
    }

    /**
     * Limpa aspas - ClientAggMapper
     */
    private static String trimQ(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.startsWith("\"") && t.endsWith("\"") && t.length() >= 2) {
            t = t.substring(1, t.length() - 1);
        }
        return t.trim();
    }

    /**
     * Converte valor monetario para centavos - ClientAggMapper
     */
    private static long parseAmountToCents(String raw) {
        if (raw == null || raw.trim().isEmpty()) return Long.MIN_VALUE;
        try {
            String clean = raw.trim().replace("\"", "").replace("$", "").replace(" ", "").replace(",", "");
            if (clean.isEmpty()) return Long.MIN_VALUE;
            BigDecimal bd = new BigDecimal(clean);
            BigDecimal cents = bd.movePointRight(2);
            return cents.setScale(0, RoundingMode.HALF_UP).longValueExact();
        } catch (Exception e) {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Split de CSV - ClientAggMapper
     */
    private static String[] splitCsv(String line) {
        List<String> res = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        boolean inQuotes = false;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            if (ch == '\"') {
                inQuotes = !inQuotes;
            } else if (ch == ',' && !inQuotes) {
                res.add(cur.toString());
                cur.setLength(0);
            } else {
                cur.append(ch);
            }
        }
        res.add(cur.toString());
        return res.toArray(new String[0]);
    }
}