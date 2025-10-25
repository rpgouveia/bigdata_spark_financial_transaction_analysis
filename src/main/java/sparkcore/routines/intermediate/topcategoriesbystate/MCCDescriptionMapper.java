package sparkcore.routines.intermediate.topcategoriesbystate;

import java.util.HashMap;
import java.util.Map;

/**
 * Classe utilitária para mapear códigos MCC para suas descrições
 * Baseada nos códigos do dataset
 */
public class MCCDescriptionMapper {

    // Mapa estático com códigos MCC e descrições
    private static final Map<String, String> MCC_DESCRIPTIONS = new HashMap<>();

    static {
        // Inicializar o mapa com os códigos MCC mais comuns do dataset
        MCC_DESCRIPTIONS.put("5812", "Restaurants");
        MCC_DESCRIPTIONS.put("5541", "Service Stations");
        MCC_DESCRIPTIONS.put("7996", "Amusement Parks");
        MCC_DESCRIPTIONS.put("5411", "Supermarkets");
        MCC_DESCRIPTIONS.put("4784", "Tolls/Bridge Fees");
        MCC_DESCRIPTIONS.put("4900", "Utilities");
        MCC_DESCRIPTIONS.put("5942", "Book Stores");
        MCC_DESCRIPTIONS.put("5814", "Fast Food");
        MCC_DESCRIPTIONS.put("4829", "Money Transfer");
        MCC_DESCRIPTIONS.put("5311", "Department Stores");
        MCC_DESCRIPTIONS.put("5211", "Lumber/Building");
        MCC_DESCRIPTIONS.put("5310", "Discount Stores");
        MCC_DESCRIPTIONS.put("3780", "Network Services");
        MCC_DESCRIPTIONS.put("5499", "Misc Food Stores");
        MCC_DESCRIPTIONS.put("4121", "Taxis/Limos");
        MCC_DESCRIPTIONS.put("5300", "Wholesale Clubs");
        MCC_DESCRIPTIONS.put("5719", "Home Furnishing");
        MCC_DESCRIPTIONS.put("7832", "Movie Theaters");
        MCC_DESCRIPTIONS.put("5813", "Bars/Pubs");
        MCC_DESCRIPTIONS.put("4814", "Telecom Services");
        MCC_DESCRIPTIONS.put("5661", "Shoe Stores");
        MCC_DESCRIPTIONS.put("5977", "Cosmetics");
        MCC_DESCRIPTIONS.put("8099", "Medical Services");
        MCC_DESCRIPTIONS.put("7538", "Auto Service");
        MCC_DESCRIPTIONS.put("5912", "Pharmacies");
        MCC_DESCRIPTIONS.put("4111", "Transit");
        MCC_DESCRIPTIONS.put("5815", "Digital Media");
        MCC_DESCRIPTIONS.put("8021", "Dentists");
        MCC_DESCRIPTIONS.put("5921", "Liquor Stores");
        MCC_DESCRIPTIONS.put("5655", "Sports Apparel");
        MCC_DESCRIPTIONS.put("7230", "Beauty Salons");
        MCC_DESCRIPTIONS.put("5651", "Clothing");
        MCC_DESCRIPTIONS.put("4899", "Cable/Satellite TV");
        MCC_DESCRIPTIONS.put("5251", "Hardware Stores");
        MCC_DESCRIPTIONS.put("7995", "Gambling/Betting");
        MCC_DESCRIPTIONS.put("7011", "Hotels/Lodging");
        MCC_DESCRIPTIONS.put("5732", "Electronics");
        MCC_DESCRIPTIONS.put("5712", "Furniture");
        MCC_DESCRIPTIONS.put("5816", "Digital Games");
        MCC_DESCRIPTIONS.put("5941", "Sporting Goods");
        MCC_DESCRIPTIONS.put("5722", "Appliances");
        MCC_DESCRIPTIONS.put("5733", "Music Stores");
        MCC_DESCRIPTIONS.put("5947", "Gift Shops");
        MCC_DESCRIPTIONS.put("4511", "Airlines");
        MCC_DESCRIPTIONS.put("5533", "Auto Parts");
        MCC_DESCRIPTIONS.put("4722", "Travel Agencies");
        MCC_DESCRIPTIONS.put("6300", "Insurance");
        MCC_DESCRIPTIONS.put("8011", "Doctors");
        MCC_DESCRIPTIONS.put("8111", "Legal Services");
        MCC_DESCRIPTIONS.put("8062", "Hospitals");
        MCC_DESCRIPTIONS.put("8931", "Accounting");
    }

    /**
     * Obtém a descrição resumida para um código MCC
     * @param mccCode Código MCC
     * @return Descrição resumida do MCC
     */
    public static String getDescription(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty()) {
            return "Unknown";
        }

        String description = MCC_DESCRIPTIONS.get(mccCode.trim());
        if (description != null) {
            return description;
        } else {
            return "MCC-" + mccCode;
        }
    }

    /**
     * Verifica se o código MCC é conhecido
     * @param mccCode Código MCC
     * @return true se o código é conhecido
     */
    public static boolean isKnownMCC(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty()) {
            return false;
        }
        return MCC_DESCRIPTIONS.containsKey(mccCode.trim());
    }
}