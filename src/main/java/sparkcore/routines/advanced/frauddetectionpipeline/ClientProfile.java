package sparkcore.routines.advanced.frauddetectionpipeline;

import java.io.Serializable;
import java.util.Locale;

/**
 * Classe que representa o perfil agregado de um cliente.
 * Armazena estatisticas comportamentais para analise de risco.
 * Versao Serializable para uso com Spark.
 */
public class ClientProfile implements Serializable {

    private String clientId;
    private int transactionCount;
    private double totalAmount;
    private double avgAmount;
    private int uniqueCities;
    private int uniqueMccs;
    private int uniqueCards;
    private long firstTransaction;
    private long lastTransaction;
    private int onlineCount;
    private int swipeCount;
    private int errorCount;
    private int chargebackCount;

    public ClientProfile() {
        this.clientId = "";
        this.transactionCount = 0;
        this.totalAmount = 0.0;
        this.avgAmount = 0.0;
        this.uniqueCities = 0;
        this.uniqueMccs = 0;
        this.uniqueCards = 0;
        this.firstTransaction = 0L;
        this.lastTransaction = 0L;
        this.onlineCount = 0;
        this.swipeCount = 0;
        this.errorCount = 0;
        this.chargebackCount = 0;
    }

    public ClientProfile(String clientId, int transactionCount, double totalAmount,
                         double avgAmount, int uniqueCities, int uniqueMccs,
                         int uniqueCards, long firstTransaction, long lastTransaction,
                         int onlineCount, int swipeCount, int errorCount,
                         int chargebackCount) {
        this.clientId = clientId;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
        this.avgAmount = avgAmount;
        this.uniqueCities = uniqueCities;
        this.uniqueMccs = uniqueMccs;
        this.uniqueCards = uniqueCards;
        this.firstTransaction = firstTransaction;
        this.lastTransaction = lastTransaction;
        this.onlineCount = onlineCount;
        this.swipeCount = swipeCount;
        this.errorCount = errorCount;
        this.chargebackCount = chargebackCount;
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "%d\t%.2f\t%.2f\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d",
                transactionCount, totalAmount, avgAmount, uniqueCities,
                uniqueMccs, uniqueCards, firstTransaction, lastTransaction,
                onlineCount, swipeCount, errorCount, chargebackCount);
    }

    // Getters
    public String getClientId() { return clientId; }
    public int getTransactionCount() { return transactionCount; }
    public double getTotalAmount() { return totalAmount; }
    public double getAvgAmount() { return avgAmount; }
    public int getUniqueCities() { return uniqueCities; }
    public int getUniqueMccs() { return uniqueMccs; }
    public int getUniqueCards() { return uniqueCards; }
    public long getFirstTransaction() { return firstTransaction; }
    public long getLastTransaction() { return lastTransaction; }
    public int getOnlineCount() { return onlineCount; }
    public int getSwipeCount() { return swipeCount; }
    public int getErrorCount() { return errorCount; }
    public int getChargebackCount() { return chargebackCount; }
}