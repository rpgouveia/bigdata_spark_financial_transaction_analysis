package sparkcore.routines.advanced.frauddetectionpipeline;

import java.io.Serializable;
import java.util.Locale;

/**
 * Classe que representa a classificacao de risco de um cliente.
 * Armazena score e categoria de risco.
 * Versao Serializable para uso com Spark.
 */
public class ClientRisk implements Serializable, Comparable<ClientRisk> {

    private String clientId;
    private String riskCategory; // LOW, MEDIUM, HIGH, CRITICAL
    private double riskScore;
    private String riskFactors;
    private int transactionCount;
    private double totalAmount;

    public ClientRisk() {
        this.clientId = "";
        this.riskCategory = "";
        this.riskScore = 0.0;
        this.riskFactors = "";
        this.transactionCount = 0;
        this.totalAmount = 0.0;
    }

    public ClientRisk(String clientId, String riskCategory, double riskScore,
                      String riskFactors, int transactionCount, double totalAmount) {
        this.clientId = clientId;
        this.riskCategory = riskCategory;
        this.riskScore = riskScore;
        this.riskFactors = riskFactors;
        this.transactionCount = transactionCount;
        this.totalAmount = totalAmount;
    }

    @Override
    public int compareTo(ClientRisk other) {
        // Ordena por risk score decrescente
        return Double.compare(other.riskScore, this.riskScore);
    }

    @Override
    public String toString() {
        return String.format(Locale.US, "%s\t%.2f\t%s\t%d\t%.2f",
                clientId, riskScore, riskFactors, transactionCount, totalAmount);
    }

    // Getters
    public String getClientId() { return clientId; }
    public String getRiskCategory() { return riskCategory; }
    public double getRiskScore() { return riskScore; }
    public String getRiskFactors() { return riskFactors; }
    public int getTransactionCount() { return transactionCount; }
    public double getTotalAmount() { return totalAmount; }
}