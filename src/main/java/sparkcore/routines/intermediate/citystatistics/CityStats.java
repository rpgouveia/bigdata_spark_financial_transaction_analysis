package sparkcore.routines.intermediate.citystatistics;

import java.io.Serializable;

/**
 * Classe auxiliar para armazenar estatísticas de transações por cidade
 * Similar ao Custom Writable do Hadoop, mas usando Spark Core
 */
public class CityStats implements Serializable {

    private long transactionCount;      // Número de transações
    private long totalAmountInCents;    // Valor total em centavos

    /**
     * Construtor padrão
     */
    public CityStats() {
        this.transactionCount = 0;
        this.totalAmountInCents = 0;
    }

    /**
     * Construtor com valores iniciais
     */
    public CityStats(long count, long totalInCents) {
        this.transactionCount = count;
        this.totalAmountInCents = totalInCents;
    }

    // Getters
    public long getTransactionCount() {
        return transactionCount;
    }

    public long getTotalAmountInCents() {
        return totalAmountInCents;
    }

    /**
     * Calcula o valor médio por transação em centavos
     */
    public long getAverageAmountInCents() {
        if (transactionCount == 0) {
            return 0;
        }
        return totalAmountInCents / transactionCount;
    }

    /**
     * Retorna o total em dólares
     */
    public double getTotalAmountInDollars() {
        return totalAmountInCents / 100.0;
    }

    /**
     * Retorna a média em dólares
     */
    public double getAverageAmountInDollars() {
        return getAverageAmountInCents() / 100.0;
    }

    /**
     * Adiciona os valores de outro CityStats a este
     * Usado na agregação (equivalente ao reduce)
     */
    public CityStats add(CityStats other) {
        return new CityStats(
                this.transactionCount + other.transactionCount,
                this.totalAmountInCents + other.totalAmountInCents
        );
    }

    /**
     * ToString para output formatado
     */
    @Override
    public String toString() {
        return String.format("Transações: %d | Total: $%.2f | Média: $%.2f",
                transactionCount,
                getTotalAmountInDollars(),
                getAverageAmountInDollars());
    }

    /**
     * Formato compacto separado por vírgulas (para CSV)
     */
    public String toCSV() {
        return String.format("%d,%.2f,%.2f",
                transactionCount,
                getTotalAmountInDollars(),
                getAverageAmountInDollars());
    }
}