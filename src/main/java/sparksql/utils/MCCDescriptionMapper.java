package sparksql.utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Classe utilitária para mapear códigos MCC para suas descrições
 * Versão para Spark SQL - compatível com UDF (User Defined Functions)
 *
 * Baseada nos códigos MCC presentes no dataset de transações financeiras.
 * Esta classe é reutilizável em múltiplas rotinas Spark SQL.
 */
public class MCCDescriptionMapper {

    // Mapa estático com códigos MCC e descrições
    private static final Map<String, String> MCC_DESCRIPTIONS = new HashMap<>();

    static {
        // Inicializar o mapa com todos os códigos MCC do dataset
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
        MCC_DESCRIPTIONS.put("3390", "Metalwork");
        MCC_DESCRIPTIONS.put("7922", "Theater");
        MCC_DESCRIPTIONS.put("3722", "Railways");
        MCC_DESCRIPTIONS.put("5651", "Clothing");
        MCC_DESCRIPTIONS.put("4899", "Cable/Satellite TV");
        MCC_DESCRIPTIONS.put("5251", "Hardware Stores");
        MCC_DESCRIPTIONS.put("7995", "Gambling/Betting");
        MCC_DESCRIPTIONS.put("3596", "Machinery Mfg");
        MCC_DESCRIPTIONS.put("3730", "Ship Services");
        MCC_DESCRIPTIONS.put("9402", "Postal Services");
        MCC_DESCRIPTIONS.put("7801", "Sports Venues");
        MCC_DESCRIPTIONS.put("5970", "Art Supplies");
        MCC_DESCRIPTIONS.put("5932", "Antique Shops");
        MCC_DESCRIPTIONS.put("5621", "Women's Clothing");
        MCC_DESCRIPTIONS.put("7349", "Cleaning Services");
        MCC_DESCRIPTIONS.put("4722", "Travel Agencies");
        MCC_DESCRIPTIONS.put("5193", "Florists");
        MCC_DESCRIPTIONS.put("3775", "Railroad Freight");
        MCC_DESCRIPTIONS.put("3684", "Semiconductors");
        MCC_DESCRIPTIONS.put("5045", "Computer Equipment");
        MCC_DESCRIPTIONS.put("3504", "Garden Supplies");
        MCC_DESCRIPTIONS.put("7011", "Hotels/Lodging");
        MCC_DESCRIPTIONS.put("8041", "Chiropractors");
        MCC_DESCRIPTIONS.put("4214", "Trucking");
        MCC_DESCRIPTIONS.put("6300", "Insurance");
        MCC_DESCRIPTIONS.put("8011", "Doctors");
        MCC_DESCRIPTIONS.put("3509", "Industrial Supply");
        MCC_DESCRIPTIONS.put("7210", "Laundry");
        MCC_DESCRIPTIONS.put("5192", "Books/Periodicals");
        MCC_DESCRIPTIONS.put("7542", "Car Washes");
        MCC_DESCRIPTIONS.put("3640", "Lighting/Electric");
        MCC_DESCRIPTIONS.put("7393", "Security Services");
        MCC_DESCRIPTIONS.put("8111", "Legal Services");
        MCC_DESCRIPTIONS.put("3771", "Railroad Passenger");
        MCC_DESCRIPTIONS.put("5732", "Electronics");
        MCC_DESCRIPTIONS.put("5094", "Jewelry/Precious");
        MCC_DESCRIPTIONS.put("5712", "Furniture");
        MCC_DESCRIPTIONS.put("5816", "Digital Games");
        MCC_DESCRIPTIONS.put("7802", "Sports Clubs");
        MCC_DESCRIPTIONS.put("3389", "Metal Services");
        MCC_DESCRIPTIONS.put("8043", "Optometrists");
        MCC_DESCRIPTIONS.put("3393", "Heat Treating");
        MCC_DESCRIPTIONS.put("3174", "Upholstery");
        MCC_DESCRIPTIONS.put("3001", "Steel Mfg");
        MCC_DESCRIPTIONS.put("3395", "Welding");
        MCC_DESCRIPTIONS.put("3058", "Tools/Parts Mfg");
        MCC_DESCRIPTIONS.put("8049", "Podiatrists");
        MCC_DESCRIPTIONS.put("3387", "Plating Services");
        MCC_DESCRIPTIONS.put("4112", "Railways");
        MCC_DESCRIPTIONS.put("3405", "Ironwork");
        MCC_DESCRIPTIONS.put("5261", "Garden Stores");
        MCC_DESCRIPTIONS.put("3144", "Floor Covering");
        MCC_DESCRIPTIONS.put("3132", "Leather Goods");
        MCC_DESCRIPTIONS.put("3359", "Metal Foundries");
        MCC_DESCRIPTIONS.put("8931", "Accounting");
        MCC_DESCRIPTIONS.put("8062", "Hospitals");
        MCC_DESCRIPTIONS.put("7276", "Tax Services");
        MCC_DESCRIPTIONS.put("4131", "Bus Lines");
        MCC_DESCRIPTIONS.put("3260", "Pottery/Ceramics");
        MCC_DESCRIPTIONS.put("3256", "Building Materials");
        MCC_DESCRIPTIONS.put("3006", "Metal Fabrication");
        MCC_DESCRIPTIONS.put("7531", "Auto Body Repair");
        MCC_DESCRIPTIONS.put("1711", "HVAC Contractors");
        MCC_DESCRIPTIONS.put("5947", "Gift Shops");
        MCC_DESCRIPTIONS.put("3007", "Laminated Products");
        MCC_DESCRIPTIONS.put("4511", "Airlines");
        MCC_DESCRIPTIONS.put("3075", "Fasteners Mfg");
        MCC_DESCRIPTIONS.put("3066", "Misc Metals");
        MCC_DESCRIPTIONS.put("3005", "Metal Fabrication");
        MCC_DESCRIPTIONS.put("4411", "Cruise Lines");
        MCC_DESCRIPTIONS.put("3000", "Steelworks");
        MCC_DESCRIPTIONS.put("5533", "Auto Parts");
        MCC_DESCRIPTIONS.put("3008", "Steel Drums");
        MCC_DESCRIPTIONS.put("7549", "Towing");
        MCC_DESCRIPTIONS.put("5941", "Sporting Goods");
        MCC_DESCRIPTIONS.put("5722", "Appliances");
        MCC_DESCRIPTIONS.put("3009", "Structural Metal");
        MCC_DESCRIPTIONS.put("5733", "Music Stores");
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

    /**
     * Obtém a categoria geral baseada no primeiro dígito do MCC
     * @param mccCode Código MCC
     * @return Categoria geral
     */
    public static String getGeneralCategory(String mccCode) {
        if (mccCode == null || mccCode.trim().isEmpty() || mccCode.length() < 1) {
            return "Unknown";
        }

        char firstDigit = mccCode.trim().charAt(0);
        switch (firstDigit) {
            case '1':
                return "Contracted Services";
            case '2':
                return "Airlines";
            case '3':
                return "Manufacturing";
            case '4':
                return "Transportation";
            case '5':
                return "Retail";
            case '6':
                return "Financial";
            case '7':
                return "Services";
            case '8':
                return "Professional";
            case '9':
                return "Government";
            default:
                return "Other";
        }
    }

    /**
     * Retorna o número total de códigos MCC conhecidos
     * @return Total de códigos MCC no mapa
     */
    public static int getTotalMCCCodes() {
        return MCC_DESCRIPTIONS.size();
    }

    /**
     * Retorna todos os códigos MCC conhecidos
     * Útil para validação ou análises
     * @return Array com todos os códigos MCC
     */
    public static String[] getAllMCCCodes() {
        return MCC_DESCRIPTIONS.keySet().toArray(new String[0]);
    }
}