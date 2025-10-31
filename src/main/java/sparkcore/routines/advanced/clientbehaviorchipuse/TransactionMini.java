package sparkcore.routines.advanced.clientbehaviorchipuse;

import java.io.Serializable;

/**
 * Value com os campos minimos por transacao para perfilar o cliente - TransactionMiniWritable
 */
public class TransactionMini implements Serializable {

    private boolean isOnline;     // online, swipe
    private boolean hasError;     // coluna errors nao vazia
    private long amountCents;     // valor em centavos
    private String city;          // merchant_city
    private String state;         // merchant_state (UF)
    private String mcc;           // codigo MCC

    public TransactionMini() {}

    public TransactionMini(boolean isOnline, boolean hasError, long amountCents,
                           String city, String state, String mcc) {
        this.isOnline = isOnline;
        this.hasError = hasError;
        this.amountCents = amountCents;
        this.city = nz(city);
        this.state = nz(state);
        this.mcc = nz(mcc);
    }

    public boolean isOnline() { return isOnline; }
    public boolean isHasError() { return hasError; }
    public long getAmountCents() { return amountCents; }
    public String getCity() { return city; }
    public String getState() { return state; }
    public String getMcc() { return mcc; }

    private static String nz(String s) {
        return (s == null || s.trim().isEmpty()) ? "UNKNOWN" : s.trim().toUpperCase();
    }
}