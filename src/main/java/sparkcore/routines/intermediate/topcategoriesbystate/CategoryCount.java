package sparkcore.routines.intermediate.topcategoriesbystate;

import routines.intermediate.topcategoriesbycountry.MCCDescriptionMapper;

import java.io.Serializable;


/**
 * Classe auxiliar para armazenar um código MCC com sua contagem
 * Usada para ranking e comparação
 */
public class CategoryCount implements Serializable, Comparable<CategoryCount> {

    private String mccCode;
    private long count;

    public CategoryCount(String mccCode, long count) {
        this.mccCode = mccCode;
        this.count = count;
    }

    public String getMccCode() {
        return mccCode;
    }

    public long getCount() {
        return count;
    }

    /**
     * Comparação para ordenação (decrescente por count)
     */
    @Override
    public int compareTo(CategoryCount other) {
        return Long.compare(other.count, this.count);
    }

    /**
     * Formato para exibição
     */
    public String toFormattedString(int rank) {
        String description = MCCDescriptionMapper.getDescription(mccCode);
        return String.format("Top-%d: %s (%s) %d", rank, mccCode, description, count);
    }

    @Override
    public String toString() {
        return mccCode + ":" + count;
    }
}