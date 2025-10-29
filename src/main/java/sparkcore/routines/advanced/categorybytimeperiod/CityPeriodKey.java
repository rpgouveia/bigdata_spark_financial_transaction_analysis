package sparkcore.routines.advanced.categorybytimeperiod;

import java.io.Serializable;
import java.util.Objects;

/**
 * Chave composta para agrupar por Cidade + Periodo do Dia
 * Permite identificar categorias especificas por horario em cada cidade
 * Versao Serializable para uso com Spark
 */
public class CityPeriodKey implements Serializable, Comparable<CityPeriodKey> {

    private String cityName;      // Nome da cidade
    private String timePeriod;    // Periodo: MORNING, AFTERNOON, NIGHT

    /**
     * Construtor padrao
     */
    public CityPeriodKey() {
        this.cityName = "";
        this.timePeriod = "";
    }

    /**
     * Construtor com valores iniciais
     */
    public CityPeriodKey(String cityName, String timePeriod) {
        this.cityName = cityName;
        this.timePeriod = timePeriod;
    }

    // Getters e Setters
    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getTimePeriod() {
        return timePeriod;
    }

    public void setTimePeriod(String timePeriod) {
        this.timePeriod = timePeriod;
    }

    /**
     * Metodo compareTo para sorting
     * Ordena primeiro por cidade (alfabetico), depois por periodo (ordem definida)
     */
    @Override
    public int compareTo(CityPeriodKey other) {
        int cityComparison = this.cityName.compareTo(other.cityName);
        if (cityComparison != 0) {
            return cityComparison;
        }
        return getPeriodOrder(this.timePeriod) - getPeriodOrder(other.timePeriod);
    }

    /**
     * Define ordem dos periodos para sorting
     */
    private int getPeriodOrder(String period) {
        switch (period) {
            case "MORNING":
                return 1;
            case "AFTERNOON":
                return 2;
            case "NIGHT":
                return 3;
            default:
                return 4;
        }
    }

    /**
     * ToString para debugging
     */
    @Override
    public String toString() {
        return cityName + "-" + timePeriod;
    }

    /**
     * Formato legivel para output final
     */
    public String toDisplayString() {
        String periodName;
        switch (timePeriod) {
            case "MORNING":
                periodName = "Manha";
                break;
            case "AFTERNOON":
                periodName = "Tarde";
                break;
            case "NIGHT":
                periodName = "Noite";
                break;
            default:
                periodName = timePeriod;
        }
        return String.format("%s [%s]", cityName, periodName);
    }

    /**
     * Equals para comparacao
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        CityPeriodKey that = (CityPeriodKey) obj;
        return cityName.equals(that.cityName) && timePeriod.equals(that.timePeriod);
    }

    /**
     * HashCode para uso em colecoes
     */
    @Override
    public int hashCode() {
        return Objects.hash(cityName, timePeriod);
    }
}