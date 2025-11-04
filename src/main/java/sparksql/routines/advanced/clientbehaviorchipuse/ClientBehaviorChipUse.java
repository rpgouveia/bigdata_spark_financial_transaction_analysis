package sparksql.routines.advanced.clientbehaviorchipuse;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static org.apache.spark.sql.functions.*;

// Para executar configure os argumentos da seguinte forma:
// src/main/resources/transactions_data.csv output/spark_sql/advanced/client_behavior_chip_use

/**
 * Rotina avancada que analisa o comportamento do cliente baseado em transacoes.
 * Usa Spark SQL (DataFrames) para processar os dados.
 * Objetivo:
 * 1. Perfilar cada cliente (Job 1):
 * - Calcular metricas: onlineRate, errorRate, avgCents, maxCents.
 * - Determinar UF (Estado) predominante do cliente.
 * - Classificar o cliente em Risco (LOW, MED, HIGH).
 *
 * 2. Agregar por Estado (Job 2):
 * - Contar clientes por Risco (LOW, MED, HIGH) em cada UF.
 * - Identificar as Top 5 cidades com mais clientes de ALTO Risco em cada UF.
 *
 * 3. Ordenar:
 * - O resultado final e ordenado pela porcentagem de clientes de ALTO Risco (decrescente).
 *
 * (MODIFICADO): Salva a saida como CSV padrao.
 */
public class ClientBehaviorChipUse {

    // Parametros de Risco
    private static final float RISK_ERROR_HIGH = 0.05f; // 5%
    private static final float RISK_ERROR_MED = 0.02f; // 2%
    private static final float RISK_ONLINE_HIGH = 0.80f;
    private static final float RISK_ONLINE_MED = 0.60f;
    private static final long RISK_AVG_HIGH_CENTS = 10000L; // medio $100
    private static final long RISK_MAX_HIGH_CENTS = 50000L; // alto $500
    private static final int TOPN_CITIES = 5;


    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: ClientBehaviorChipUse <input-path> <output-path>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        // Cria sessao Spark
        SparkSession spark = SparkSession.builder()
                .appName("ClientBehaviorChipUse")
                .master("local[*]")
                // Aumentar particoes para joins e agregacoes
                .config("spark.sql.shuffle.partitions", "16")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // Registro de UDFs
        // Converter valor monetario para centavos
        spark.udf().register("parseCentsUDF", (String raw) -> {
            if (raw == null || raw.trim().isEmpty()) return null;
            try {
                String clean = raw.trim().replace("\"", "").replace("$", "").replace(" ", "").replace(",", "");
                if (clean.isEmpty()) return null;
                BigDecimal bd = new BigDecimal(clean);
                BigDecimal cents = bd.movePointRight(2);
                return cents.setScale(0, RoundingMode.HALF_UP).longValueExact();
            } catch (Exception e) {
                return null;
            }
        }, DataTypes.LongType);

        // Classificar o risco do cliente
        spark.udf().register("classifyBucketUDF",
                (Double onlineRate, Double errorRate, Long avgCents, Long maxCents) -> {
                    if (onlineRate == null || errorRate == null || avgCents == null || maxCents == null) {
                        return "LOW"; // Default
                    }

                    boolean highRisk =
                            (errorRate >= RISK_ERROR_HIGH && onlineRate >= RISK_ONLINE_MED) ||
                                    (onlineRate >= RISK_ONLINE_HIGH) ||
                                    (avgCents >= RISK_AVG_HIGH_CENTS) ||
                                    (maxCents >= RISK_MAX_HIGH_CENTS);

                    if (highRisk) return "HIGH";

                    boolean medium = (errorRate >= RISK_ERROR_MED) || (onlineRate >= RISK_ONLINE_MED);
                    return medium ? "MED" : "LOW";
                }, DataTypes.StringType);

        // ===============================================

        System.out.println("========================================");
        System.out.println("Iniciando ClientBehaviorChipUse");
        System.out.println("Perfil do cliente: online vs. falhas");
        System.out.println("========================================");

        long startTime = System.currentTimeMillis();

        // Le o arquivo CSV
        Dataset<Row> rawData = spark.read()
                .option("header", "true")
                .csv(inputPath);

        // Passo 1: Limpar e preparar transacoes
        Column nz_state = upper(coalesce(trim(col("merchant_state")), lit("UNKNOWN")));
        Column nz_city = upper(coalesce(trim(col("merchant_city")), lit("UNKNOWN")));

        Dataset<Row> transactions = rawData
                // Seleciona e limpa colunas
                .withColumn("client_id", trim(col("client_id")))
                .withColumn("amount_cents", call_udf("parseCentsUDF", col("amount")))
                .withColumn("is_online", when(upper(trim(col("use_chip"))).equalTo("ONLINE TRANSACTION"), 1).otherwise(0))
                .withColumn("has_error", when(col("errors").isNotNull().and(trim(col("errors")).notEqual("")), 1).otherwise(0))
                .withColumn("state", nz_state)
                .withColumn("city", nz_city)
                // Filtra dados invalidos
                .filter(col("client_id").isNotNull()
                        .and(col("client_id").notEqual(""))
                        .and(col("amount_cents").isNotNull())
                        .and(col("state").notEqual("UNKNOWN")));

        // Cacheia o DF de transacoes limpas
        transactions.cache();

        // JOB 1: Perfilar Cliente

        // Passo 2: Calcular metricas basicas do cliente
        Dataset<Row> clientAgg = transactions
                .groupBy("client_id")
                .agg(
                        count(lit(1)).as("tx"),
                        sum("is_online").as("online_count"),
                        sum("has_error").as("error_count"),
                        sum("amount_cents").as("sum_cents"),
                        max("amount_cents").as("max_cents")
                );

        // Passo 3: Encontrar o Estado (UF) predominante do cliente
        WindowSpec clientStateWindow = Window.partitionBy("client_id").orderBy(desc("freq"));
        Dataset<Row> clientTopState = transactions
                .groupBy("client_id", "state")
                .agg(count(lit(1)).as("freq"))
                .withColumn("rn", row_number().over(clientStateWindow))
                .filter(col("rn").equalTo(1))
                .select(col("client_id"), col("state").as("top_state"));

        // Passo 4: Encontrar a Cidade predominante do cliente
        WindowSpec clientCityWindow = Window.partitionBy("client_id").orderBy(desc("freq"));
        Dataset<Row> clientTopCity = transactions
                .groupBy("client_id", "city")
                .agg(count(lit(1)).as("freq"))
                .withColumn("rn", row_number().over(clientCityWindow))
                .filter(col("rn").equalTo(1))
                .select(col("client_id"), col("city").as("top_city"));

        // Passo 5: Montar Perfil do Cliente e Classificar Risco
        Dataset<Row> clientProfile = clientAgg
                .join(clientTopState, "client_id")
                .join(clientTopCity, "client_id")
                .withColumn("online_rate", col("online_count").divide(col("tx")))
                .withColumn("error_rate", col("error_count").divide(col("tx")))
                .withColumn("avg_cents", col("sum_cents").divide(col("tx")).cast(DataTypes.LongType))
                .withColumn("risk_bucket", call_udf("classifyBucketUDF",
                        col("online_rate"), col("error_rate"), col("avg_cents"), col("max_cents")
                ));

        // Cacheia o perfil do cliente, pois sera usado 2x
        clientProfile.cache();

        System.out.println("========================================");
        System.out.println("Agregacao por Cliente concluida.");
        System.out.println("========================================");

        // JOB 2: Agregar por Estado

        // Passo 6: Contar clientes por Risco em cada Estado
        Dataset<Row> stateAgg = clientProfile
                .groupBy("top_state")
                .agg(
                        count(lit(1)).as("total_clients"),
                        sum(when(col("risk_bucket").equalTo("LOW"), 1).otherwise(0)).as("low_risk_clients"),
                        sum(when(col("risk_bucket").equalTo("MED"), 1).otherwise(0)).as("med_risk_clients"),
                        sum(when(col("risk_bucket").equalTo("HIGH"), 1).otherwise(0)).as("high_risk_clients")
                );

        // Passo 7: Identificar Top 5 Cidades com clientes de ALTO Risco
        // Contar clientes de Alto Risco por (Estado, Cidade)
        Dataset<Row> highRiskCityCounts = clientProfile
                .filter(col("risk_bucket").equalTo("HIGH"))
                .groupBy("top_state", "top_city")
                .agg(count(lit(1)).as("high_risk_count"));

        // Rankear cidades dentro de cada estado
        WindowSpec stateCityRankWindow = Window.partitionBy("top_state").orderBy(desc("high_risk_count"));
        Dataset<Row> topCities = highRiskCityCounts
                .withColumn("rn", row_number().over(stateCityRankWindow))
                .filter(col("rn").leq(TOPN_CITIES))
                // Formatar string "Cidade: Contagem"
                .withColumn("city_summary", format_string("%s: %d", col("top_city"), col("high_risk_count")))
                // Agrupar strings por estado
                .groupBy("top_state")
                .agg(
                        array_join(
                                transform(
                                        sort_array(
                                                collect_list(struct(col("rn"), col("city_summary"))),
                                                true
                                        ),
                                        s -> s.getField("city_summary")
                                ),
                                " | "
                        ).as("top_cities_string")
                );

        // Passo 8: Juntar agregados do Estado com Top Cidades
        Dataset<Row> finalResults = stateAgg
                .join(topCities, "top_state", "left_outer")
                // Calcula as porcentagens como numeros (Double)
                .withColumn("high_risk_pct", col("high_risk_clients").divide(col("total_clients")))
                .withColumn("med_risk_pct", col("med_risk_clients").divide(col("total_clients")))
                .withColumn("low_risk_pct", col("low_risk_clients").divide(col("total_clients")))
                // Prepara a string das cidades
                .withColumn("TopCitiesSummary", coalesce(col("top_cities_string"), lit("N/A")))
                // Ordena pela % de Alto Risco (descendente)
                .orderBy(desc("high_risk_pct"));

        // Passo 9: Selecionar colunas finais e salvar como CSV (MODIFICADO)
        Dataset<Row> csvOutput = finalResults
                .select(
                        col("top_state").as("State"),
                        col("total_clients").as("TotalClients"),
                        col("low_risk_clients").as("LowRiskClients"),
                        col("med_risk_clients").as("MedRiskClients"),
                        col("high_risk_clients").as("HighRiskClients"),
                        // Arredonda as porcentagens para 4 casas decimais para o CSV
                        round(col("low_risk_pct"), 4).as("LowRiskPct"),
                        round(col("med_risk_pct"), 4).as("MedRiskPct"),
                        round(col("high_risk_pct"), 4).as("HighRiskPct"),
                        col("TopCitiesSummary")
                );

        // Salva o resultado como CSV
        csvOutput
                .coalesce(1)
                .write()
                .mode("overwrite")
                .option("header", "true")
                .csv(outputPath);

        long endTime = System.currentTimeMillis();
        System.out.println("========================================");
        System.out.println("Agregacao por Estado concluida.");
        System.out.println("Tempo total de processamento: " + (endTime - startTime) + "ms");
        System.out.println("Resultados salvos em: " + outputPath);
        System.out.println("========================================");

        spark.stop();
    }
}