package sparkcore.routines.advanced.frauddetectionpipeline;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/advanced/fraud_detection_pipeline

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Pipeline de analise de risco de fraudes usando Spark Core (RDDs).
 * Processa transacoes em um unico pipeline integrado.
 *
 * Etapas:
 * 1. Client Profile Builder - agrega transacoes por cliente
 * 2. Risk Category Classifier - calcula risk score e categoriza
 * 3. Final Risk Report Generator - gera relatorios consolidados
 *
 * Categorias de Risco:
 * - LOW (0-30 pontos): Comportamento normal
 * - MEDIUM (31-60 pontos): Alguns sinais de alerta
 * - HIGH (61-85 pontos): Multiplos indicadores de risco
 * - CRITICAL (86-100+ pontos): Risco extremo
 */
public class FraudDetectionPipeline {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: FraudDetectionPipeline <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        SparkConf conf = new SparkConf()
                .setAppName("FraudDetectionPipeline")
                .setMaster("local[*]");

        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        System.out.println("============================================================");
        System.out.println("    FRAUD DETECTION PIPELINE - SPARK RDDs");
        System.out.println("============================================================");

        long totalStartTime = System.currentTimeMillis();

        // STEP 1: CLIENT PROFILE BUILDER
        System.out.println("\n>>> STEP 1: Building Client Profiles...");
        long step1Start = System.currentTimeMillis();

        JavaRDD<String> lines = sc.textFile(inputPath);
        String header = lines.first();
        JavaRDD<String> data = lines.filter(line -> !line.equals(header) && !line.trim().isEmpty());

        JavaPairRDD<String, ClientProfile> clientProfiles = data
                .mapToPair(line -> {
                    String[] fields = line.split(",", -1);
                    if (fields.length < 12) return null;
                    String clientId = fields[2].trim();
                    return new Tuple2<>(clientId, line);
                })
                .filter(tuple -> tuple != null)
                .groupByKey()
                .mapValues(transactions -> buildClientProfile(transactions));

        long step1Duration = System.currentTimeMillis() - step1Start;
        long profileCount = clientProfiles.count();
        System.out.println(">>> STEP 1 COMPLETED in " + (step1Duration / 1000) + " seconds");
        System.out.println("    Profiles created: " + profileCount);

        // STEP 2: RISK CATEGORY CLASSIFIER
        System.out.println("\n>>> STEP 2: Classifying Risk Categories...");
        long step2Start = System.currentTimeMillis();

        JavaPairRDD<String, ClientRisk> clientRisks = clientProfiles
                .mapValues((ClientProfile profile) -> classifyRisk(profile));

        long step2Duration = System.currentTimeMillis() - step2Start;

        Map<String, Long> categoryCount = clientRisks
                .mapToPair(tuple -> new Tuple2<>(tuple._2.getRiskCategory(), 1L))
                .reduceByKey((a, b) -> a + b)
                .collectAsMap();

        System.out.println(">>> STEP 2 COMPLETED in " + (step2Duration / 1000) + " seconds");
        System.out.println("    Risk categories:");
        for (String cat : Arrays.asList("CRITICAL", "HIGH", "MEDIUM", "LOW")) {
            long count = categoryCount.getOrDefault(cat, 0L);
            System.out.println("      " + cat + ": " + count + " clients");
        }

        // STEP 3: FINAL RISK REPORT GENERATOR
        System.out.println("\n>>> STEP 3: Generating Final Risk Reports...");
        long step3Start = System.currentTimeMillis();

        JavaPairRDD<String, String> riskReports = clientRisks
                .mapToPair(tuple -> new Tuple2<>(tuple._2.getRiskCategory(), tuple._2))
                .groupByKey()
                .mapValues(risks -> generateReport(risks))
                .mapToPair(tuple -> {
                    int order = getCategoryOrder(tuple._1);
                    return new Tuple2<>(order, tuple);
                })
                .sortByKey(false)
                .mapToPair(tuple -> tuple._2);

        long step3Duration = System.currentTimeMillis() - step3Start;
        System.out.println(">>> STEP 3 COMPLETED in " + (step3Duration / 1000) + " seconds");

        // Mostra preview dos relatorios
        System.out.println("\n========================================");
        System.out.println("RISK REPORTS PREVIEW:");
        System.out.println("========================================");

        List<Tuple2<String, String>> reports = riskReports.collect();
        for (Tuple2<String, String> report : reports) {
            System.out.println(report._2);
        }

        // Salva resultados
        riskReports
                .map(tuple -> tuple._2)
                .coalesce(1)
                .saveAsTextFile(outputPath);

        long totalDuration = System.currentTimeMillis() - totalStartTime;

        System.out.println("\n============================================================");
        System.out.println("       PIPELINE EXECUTION SUMMARY");
        System.out.println("============================================================");
        System.out.println("Step 1 Duration: " + (step1Duration / 1000) + " seconds");
        System.out.println("Step 2 Duration: " + (step2Duration / 1000) + " seconds");
        System.out.println("Step 3 Duration: " + (step3Duration / 1000) + " seconds");
        System.out.println("------------------------------------------------------------");
        System.out.println("Total Duration: " + (totalDuration / 1000) + " seconds");
        System.out.println("============================================================");
        System.out.println("\nFinal Output Location: " + outputPath);

        sc.stop();
    }

    /**
     * Constroi perfil comportamental de um cliente
     */
    private static ClientProfile buildClientProfile(Iterable<String> transactions) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String clientId = "";
        int transactionCount = 0;
        double totalAmount = 0.0;
        Set<String> cities = new HashSet<>();
        Set<String> mccs = new HashSet<>();
        Set<String> cards = new HashSet<>();
        long firstTransaction = Long.MAX_VALUE;
        long lastTransaction = Long.MIN_VALUE;
        int onlineCount = 0;
        int swipeCount = 0;
        int errorCount = 0;
        int chargebackCount = 0;

        for (String transaction : transactions) {
            try {
                String[] fields = transaction.split(",", -1);
                if (fields.length < 12) continue;

                if (clientId.isEmpty()) {
                    clientId = fields[2].trim();
                }

                String dateStr = fields[1].trim();
                String cardId = fields[3].trim();
                String amountStr = fields[4].trim();
                String useChip = fields[5].trim();
                String merchantCity = fields[7].trim();
                String mcc = fields[10].trim();
                String errors = fields[11].trim();

                Date date = dateFormat.parse(dateStr);
                long timestamp = date.getTime();
                double amount = Double.parseDouble(amountStr.replace("$", ""));

                transactionCount++;
                totalAmount += Math.abs(amount);

                if (!merchantCity.isEmpty() && !merchantCity.equals("ONLINE")) {
                    cities.add(merchantCity);
                }

                if (!mcc.isEmpty()) {
                    mccs.add(mcc);
                }

                cards.add(cardId);

                if (timestamp < firstTransaction) firstTransaction = timestamp;
                if (timestamp > lastTransaction) lastTransaction = timestamp;

                if (useChip.contains("Online")) {
                    onlineCount++;
                } else {
                    swipeCount++;
                }

                if (errors != null && !errors.trim().isEmpty() &&
                        !errors.equalsIgnoreCase("null") && !errors.equals("N/A")) {
                    errorCount++;
                }

                if (amount < 0) {
                    chargebackCount++;
                }

            } catch (Exception e) {
                // Ignora erros de parsing
            }
        }

        double avgAmount = transactionCount > 0 ? totalAmount / transactionCount : 0.0;

        return new ClientProfile(
                clientId,
                transactionCount,
                totalAmount,
                avgAmount,
                cities.size(),
                mccs.size(),
                cards.size(),
                firstTransaction,
                lastTransaction,
                onlineCount,
                swipeCount,
                errorCount,
                chargebackCount
        );
    }

    /**
     * Classifica cliente em categoria de risco
     */
    private static ClientRisk classifyRisk(ClientProfile profile) {
        double riskScore = 0.0;
        List<String> riskFactors = new ArrayList<>();

        // Fator 1: Mobilidade (cidades diferentes)
        if (profile.getUniqueCities() > 5) {
            riskScore += 15;
            riskFactors.add("HIGH_MOBILITY[" + profile.getUniqueCities() + "_cities]");
        } else if (profile.getUniqueCities() > 3) {
            riskScore += 8;
            riskFactors.add("MEDIUM_MOBILITY[" + profile.getUniqueCities() + "_cities]");
        }

        // Fator 2: Diversidade de categorias MCC
        if (profile.getUniqueMccs() > 10) {
            riskScore += 12;
            riskFactors.add("DIVERSE_MCC[" + profile.getUniqueMccs() + "_categories]");
        } else if (profile.getUniqueMccs() > 6) {
            riskScore += 6;
            riskFactors.add("VARIED_MCC[" + profile.getUniqueMccs() + "_categories]");
        }

        // Fator 3: Multiplos cartoes
        if (profile.getUniqueCards() > 3) {
            riskScore += 20;
            riskFactors.add("MULTIPLE_CARDS[" + profile.getUniqueCards() + "_cards]");
        } else if (profile.getUniqueCards() > 1) {
            riskScore += 8;
            riskFactors.add("DUAL_CARDS[" + profile.getUniqueCards() + "_cards]");
        }

        // Fator 4: Taxa de erros
        if (profile.getTransactionCount() > 0) {
            double errorRate = (profile.getErrorCount() * 100.0) / profile.getTransactionCount();
            if (errorRate > 20) {
                riskScore += 25;
                riskFactors.add(String.format(Locale.US, "HIGH_ERROR_RATE[%.1f%%]", errorRate));
            } else if (errorRate > 10) {
                riskScore += 12;
                riskFactors.add(String.format(Locale.US, "MEDIUM_ERROR_RATE[%.1f%%]", errorRate));
            }
        }

        // Fator 5: Chargebacks
        if (profile.getChargebackCount() > 3) {
            riskScore += 25;
            riskFactors.add("FREQUENT_CHARGEBACKS[" + profile.getChargebackCount() + "]");
        } else if (profile.getChargebackCount() > 0) {
            riskScore += 10;
            riskFactors.add("CHARGEBACKS[" + profile.getChargebackCount() + "]");
        }

        // Fator 6: Valor medio alto
        if (profile.getAvgAmount() > 500) {
            riskScore += 15;
            riskFactors.add(String.format(Locale.US, "HIGH_AVG_AMOUNT[%.2f]", profile.getAvgAmount()));
        } else if (profile.getAvgAmount() > 200) {
            riskScore += 7;
            riskFactors.add(String.format(Locale.US, "MEDIUM_AVG_AMOUNT[%.2f]", profile.getAvgAmount()));
        }

        // Fator 7: Proporcao online vs presencial
        if (profile.getTransactionCount() > 0) {
            double onlineRate = (profile.getOnlineCount() * 100.0) / profile.getTransactionCount();
            if (onlineRate > 80 || onlineRate < 20) {
                riskScore += 10;
                riskFactors.add(String.format(Locale.US, "UNBALANCED_CHANNELS[%.0f%%_online]", onlineRate));
            }
        }

        // Determina categoria
        String riskCategory;
        if (riskScore >= 86) {
            riskCategory = "CRITICAL";
        } else if (riskScore >= 61) {
            riskCategory = "HIGH";
        } else if (riskScore >= 31) {
            riskCategory = "MEDIUM";
        } else {
            riskCategory = "LOW";
        }

        String factorsStr = String.join("; ", riskFactors);
        if (factorsStr.isEmpty()) {
            factorsStr = "NORMAL_BEHAVIOR";
        }

        return new ClientRisk(
                profile.getClientId(),
                riskCategory,
                riskScore,
                factorsStr,
                profile.getTransactionCount(),
                profile.getTotalAmount()
        );
    }

    /**
     * Gera relatorio consolidado por categoria
     */
    private static String generateReport(Iterable<ClientRisk> risks) {
        List<ClientRisk> clientList = new ArrayList<>();
        risks.forEach(clientList::add);

        if (clientList.isEmpty()) return "";

        String riskCategory = clientList.get(0).getRiskCategory();

        int totalClients = clientList.size();
        double totalAmount = 0.0;
        int totalTransactions = 0;
        double sumRiskScore = 0.0;

        for (ClientRisk risk : clientList) {
            totalAmount += risk.getTotalAmount();
            totalTransactions += risk.getTransactionCount();
            sumRiskScore += risk.getRiskScore();
        }

        Collections.sort(clientList);

        double avgRiskScore = sumRiskScore / totalClients;
        double avgAmount = totalAmount / totalClients;
        double avgTransactions = (double) totalTransactions / totalClients;

        StringBuilder report = new StringBuilder();

        report.append(String.format(Locale.US, "\n========== RISK CATEGORY: %s ==========\n", riskCategory));
        report.append(String.format(Locale.US, "Total Clients: %d\n", totalClients));
        report.append(String.format(Locale.US, "Average Risk Score: %.2f\n", avgRiskScore));
        report.append(String.format(Locale.US, "Total Amount: $%.2f\n", totalAmount));
        report.append(String.format(Locale.US, "Average Amount per Client: $%.2f\n", avgAmount));
        report.append(String.format(Locale.US, "Average Transactions per Client: %.1f\n", avgTransactions));
        report.append("\n");

        report.append("--- TOP 10 HIGHEST RISK CLIENTS ---\n");
        int limit = Math.min(10, clientList.size());
        for (int i = 0; i < limit; i++) {
            ClientRisk c = clientList.get(i);
            report.append(String.format(Locale.US, "%d. Client %s (Score: %.2f, Transactions: %d, Amount: $%.2f)\n   Factors: %s\n",
                    i + 1, c.getClientId(), c.getRiskScore(), c.getTransactionCount(),
                    c.getTotalAmount(), c.getRiskFactors()));
        }

        report.append("========================================\n");

        return report.toString();
    }

    /**
     * Retorna ordem de severidade da categoria
     */
    private static int getCategoryOrder(String category) {
        switch (category) {
            case "CRITICAL":
                return 4;
            case "HIGH":
                return 3;
            case "MEDIUM":
                return 2;
            case "LOW":
                return 1;
            default:
                return 0;
        }
    }
}