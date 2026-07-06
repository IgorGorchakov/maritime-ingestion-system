package com.maritime.spark.jobs;

import com.maritime.spark.SparkJobProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import static org.apache.spark.sql.functions.*;

/**
 * Batch job: p50 / p95 risk score rollup per vessel per day.
 *
 * <h3>Why percentiles?</h3>
 * Average risk score masks bimodal distributions — a vessel spending half the day
 * in a restricted zone (HIGH) and half in open water (LOW) produces a misleading
 * avg ≈ MEDIUM. p95 reveals that the vessel did experience extreme risk briefly;
 * p50 is robust to outliers. Both together give the analyst a clearer picture.
 *
 * <h3>approx_percentile vs exact percentile</h3>
 * {@code approx_percentile} uses the Greenwald-Khanna algorithm (≈ 1% relative
 * error at accuracy=100) in O(n) time and bounded memory. For risk scores in
 * [0, 100] the maximum absolute error is 1.0 — negligible for this use case.
 *
 * <h3>Why expr() and not callUDF()</h3>
 * {@code callUDF(name, ...)} resolves names only from the user-defined function
 * registry. Passing a built-in name like {@code "approx_percentile"} throws
 * {@code AnalysisException: Undefined function} at plan-analysis time.
 * {@code functions.expr(sql)} evaluates an inline SQL fragment against Spark's
 * full built-in catalog — the correct API for built-ins without a dedicated
 * typed static method in {@link org.apache.spark.sql.functions}.
 *
 * <h3>Output table: vessel_risk_percentiles</h3>
 * <pre>
 * mmsi          VARCHAR(9)
 * date          DATE
 * p50_risk      DOUBLE
 * p95_risk      DOUBLE
 * sample_count  BIGINT
 * </pre>
 *
 * <h3>Spring lifecycle</h3>
 * {@code @Order(2)} — runs after {@link DailyVesselAggregatesJob} ({@code @Order(1)}).
 */
@Component
@Order(2)
public class RiskRollupJob implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(RiskRollupJob.class);
    public static final String TARGET_TABLE = "vessel_risk_percentiles";

    private final SparkSession       spark;
    private final SparkJobProperties props;
    private final JobWriter          writer;

    public RiskRollupJob(SparkSession spark,
                         SparkJobProperties props,
                         JobWriter writer) {
        this.spark  = spark;
        this.props  = props;
        this.writer = writer;
    }

    @Override
    public void run(ApplicationArguments args) {
        log.info("RiskRollupJob starting — date={}", props.batchDate());
        execute();
        log.info("RiskRollupJob complete");
    }

    public void execute() {
        Dataset<Row> raw = spark.read()
                .format("json")
                .option("multiLine", "false")
                .load(props.inputPath());

        Dataset<Row> rollup = raw
                .filter(col("date").equalTo(props.batchDate()))
                .groupBy(
                        col("vesselEvent.mmsi").alias("mmsi"),
                        lit(props.batchDate()).cast("date").alias("date")
                )
                .agg(
                        // expr() is correct for approx_percentile — it is a Spark
                        // built-in, not a registered UDF. callUDF() would throw
                        // AnalysisException: Undefined function at plan-analysis time.
                        expr("approx_percentile(riskScore, 0.5,  100)").alias("p50_risk"),
                        expr("approx_percentile(riskScore, 0.95, 100)").alias("p95_risk"),
                        count("*").alias("sample_count")
                );

        // write() is the single Spark action — no count() after, which would
        // re-execute the full aggregation plan a second time unnecessarily.
        writer.write(rollup, TARGET_TABLE);
        log.info("RiskRollupJob wrote to {}", TARGET_TABLE);
    }
}
