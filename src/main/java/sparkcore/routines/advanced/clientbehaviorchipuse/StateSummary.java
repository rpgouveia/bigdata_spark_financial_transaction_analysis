package sparkcore.routines.advanced.clientbehaviorchipuse;

import scala.Tuple2;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Objeto de resultado final - StateSummaryWritable
 * Contem as estatisticas consolidadas por Estado e o Top N de cidades.
 *
 * Implementa Comparable para permitir a ordenacao final do RDD
 * (ordenado por % de clientes High Risk).
 */
public class StateSummary implements Serializable, Comparable<StateSummary> {

    private long totalClients;
    private long lowRiskClients;
    private long medRiskClients;
    private long highRiskClients;

    // Top N cidades por clientes High Risk
    private ArrayList<Tuple2<String, Long>> topCities;
    private int topN;

    public StateSummary(StateClientAgg agg, List<Tuple2<String, Long>> topCities, int topN) {
        this.totalClients = agg.getTotalClients();
        this.lowRiskClients = agg.getLowRiskClients();
        this.medRiskClients = agg.getMedRiskClients();
        this.highRiskClients = agg.getHighRiskClients();
        this.topCities = new ArrayList<>(topCities);
        this.topN = topN;
    }

    /**
     * Define a ordem de classificacao, ordena por % High Risk (descendente)
     */
    @Override
    public int compareTo(StateSummary o) {
        double thisPct = totalClients > 0 ? (highRiskClients * 1.0 / totalClients) : 0.0;
        double otherPct = o.totalClients > 0 ? (o.highRiskClients * 1.0 / o.totalClients) : 0.0;
        // Compara descendente
        return Double.compare(otherPct, thisPct);
    }

    /**
     * Formata a saida de texto (do StateSummaryWritable)
     */
    @Override
    public String toString() {
        double lowPct = totalClients > 0 ? (lowRiskClients * 100.0 / totalClients) : 0.0;
        double medPct = totalClients > 0 ? (medRiskClients * 100.0 / totalClients) : 0.0;
        double highPct = totalClients > 0 ? (highRiskClients * 100.0 / totalClients) : 0.0;

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("Clients: %d | Low: %d (%.2f%%) | Med: %d (%.2f%%) | High: %d (%.2f%%)",
                totalClients, lowRiskClients, lowPct, medRiskClients, medPct, highRiskClients, highPct));

        int size = Math.min(topN, topCities.size());
        if (size > 0) {
            sb.append(" | Top Cities (High Risk): ");
            for (int i = 0; i < size; i++) {
                if (i > 0) sb.append(" | ");
                Tuple2<String, Long> entry = topCities.get(i);
                sb.append(entry._1).append(": ").append(entry._2);
            }
        }
        return sb.toString();
    }
}