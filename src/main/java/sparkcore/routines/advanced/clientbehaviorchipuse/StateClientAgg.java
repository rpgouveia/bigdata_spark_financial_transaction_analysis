package sparkcore.routines.advanced.clientbehaviorchipuse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Agregado por UF (contagem de clientes Low/Med/High + Cidade->highRiskClients)
 * Usado como valor intermediario no RDD
 *
 * Esta classe e o resultado do "Job 1" e a entrada para o "Job 2"
 * Possui um metodo 'add' para ser usado no reduceByKey.
 */
public class StateClientAgg implements Serializable {

    private long totalClients;
    private long lowRiskClients;
    private long medRiskClients;
    private long highRiskClients;

    // Quantos clientes High Risk por cidade
    private Map<String, Long> highRiskCityCounts;

    public StateClientAgg() {
        this.highRiskCityCounts = new HashMap<>();
    }

    public StateClientAgg(long total, long low, long med, long high, Map<String, Long> cityHighRisk) {
        this();
        this.totalClients = total;
        this.lowRiskClients = low;
        this.medRiskClients = med;
        this.highRiskClients = high;
        if (cityHighRisk != null) {
            this.highRiskCityCounts.putAll(cityHighRisk);
        }
    }

    /**
     * Combina dois agregados (reduceByKey)
     */
    public StateClientAgg add(StateClientAgg other) {
        this.totalClients += other.totalClients;
        this.lowRiskClients += other.lowRiskClients;
        this.medRiskClients += other.medRiskClients;
        this.highRiskClients += other.highRiskClients;

        // Merge dos mapas de cidades
        other.highRiskCityCounts.forEach((city, count) -> {
            this.highRiskCityCounts.merge(city, count, Long::sum);
        });

        return this;
    }

    public static StateClientAgg singleClient(String riskBucket, String city) {
        long low = 0, med = 0, high = 0;
        if ("LOW".equals(riskBucket)) low = 1;
        else if ("MED".equals(riskBucket)) med = 1;
        else high = 1;

        Map<String, Long> hrCity = null;
        if ("HIGH".equals(riskBucket) && city != null && !city.isEmpty() && !city.equals("UNKNOWN")) {
            hrCity = new HashMap<>();
            hrCity.put(city, 1L);
        }
        return new StateClientAgg(1, low, med, high, hrCity);
    }

    public long getTotalClients() { return totalClients; }
    public long getLowRiskClients() { return lowRiskClients; }
    public long getMedRiskClients() { return medRiskClients; }
    public long getHighRiskClients() { return highRiskClients; }
    public Map<String, Long> getHighRiskCityCounts() { return highRiskCityCounts; }
}