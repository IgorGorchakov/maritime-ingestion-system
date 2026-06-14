package com.maritime.spark.jobs;

import com.maritime.spark.JobConfig;
import com.maritime.spark.SparkSessionFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

/**
 * Batch job: p50/p95 risk score rollup per vessel per day.
 *
 * <h3>Why percentiles matter here</h3>
 * Average risk score masks bimodal distributions — a vessel spending half the day
 * in a restricted zone (HIGH) and half in open water (LOW) produces misleading
 * avg ≈ MEDIUM. p95 reveals that the vessel did experience extreme risk even if
 * it was brief; p50 is robust to outliers. Both together give the analyst a
 * clearer picture than a single mean.
 *
 * <h3>approx_percentile vs exact percentile</h3>
 * Spark's {@code approx_percentile} uses the Greenwald-Khanna algorithm (0.01%
 * relative error by default) and runs in O(n) time and O(1/ε log(1/δ)) memory.
 * For the vessel risk use case the approximation error is negligible — the scores
 * are floats with a practical range of [0, 100]; 0.01% of 100 = 0.01.
 *
 * <h3>Why expr() and not callUDF()</h3>
 * {@code callUDF(name, ...)} resolves names only from the user-defined function
 * registry ({@code SparkSession.udf().register(...)}). Passing a built-in function
 * name like {@code "approx_percentile"} compiles fine but throws
 * {@code AnalysisException: Undefined function: approx_percentile} at plan
 * analysis time. {@code functions.expr(sql)} evaluates an inline SQL fragment
 * against Spark's full function catalog — both built-ins and UDFs — which is the
 * correct API for any function that lacks a dedicated typed static method in the
 * {@link org.apache.spark.sql.functions} class.
 *
 * <h3>Output table: vessel_risk_percentiles</h3>
 * <pre>
 * mmsi         VARCHAR(9)
 * date         DATE
 * p50_risk     DOUBLE
 * p95_risk     DOUBLE
 * sample_count BIGINT
 * </pre>
 */
public class RiskRollupJob {

    private static final Logger log = LoggerFactory.getLogger(RiskRollupJob.class);
    static final String TARGET_TABLE = "vessel_risk_percentiles";

    public static void main(String[] args) {
        JobConfig cfg = JobConfig.fromEnv();
        log.info("RiskRollupJob starting — date={}", cfg.batchDate);

        SparkSession spark = SparkSessionFactory.createLocal(
                "RiskRollup-" + cfg.batchDate);

        run(spark, cfg);
        spark.stop();
        log.info("RiskRollupJob complete");
    }

    public static void run(SparkSession spark, JobConfig cfg) {
        Dataset<Row> raw = spark.read()
                .format("parquet")
                .option("mergeSchema", "false")
                .load(cfg.parquetInputPath());

        // approx_percentile(col, percentile, accuracy) — accuracy=100 gives
        // ~1% relative error which is plenty for a 0-100 risk score.
        //
        // expr() evaluates an inline SQL fragment against Spark's full built-in
        // function catalog. This is correct for approx_percentile because it has
        // no dedicated typed static method in functions.*  and is NOT a UDF —
        // callUDF() would throw AnalysisException: Undefined function at runtime.
        Dataset<Row> rollup = raw
                .filter(col("date").equalTo(cfg.batchDate))
                .groupBy(
                        col("vesselEvent.mmsi").alias("mmsi"),
                        lit(cfg.batchDate).cast("date").alias("date")
                )
                .agg(
                        expr("approx_percentile(riskScore, 0.5,  100)").alias("p50_risk"),
                        expr("approx_percentile(riskScore, 0.95, 100)").alias("p95_risk"),
                        count("*").alias("sample_count")
                );

        DailyVesselAggregatesJob.writeToPostgres(rollup, TARGET_TABLE, cfg);
        log.info("Wrote {} risk percentile rows to {}", rollup.count(), TARGET_TABLE);
    }
}
