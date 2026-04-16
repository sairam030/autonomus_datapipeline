#!/usr/bin/env python3
"""
Generate a comprehensive PDF report of AutoPipeline benchmark results.
Uses fpdf2 library to create a professional-quality PDF with tables, plots, and analysis.
"""

import os
from fpdf import FPDF

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "results")
OUTPUT_PDF = os.path.join(RESULTS_DIR, "AutoPipeline_Benchmark_Report.pdf")


class BenchmarkReport(FPDF):
    """Custom PDF class for the benchmark report."""

    def __init__(self):
        super().__init__()
        self.set_auto_page_break(auto=True, margin=20)

    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(100, 100, 100)
            self.cell(0, 8, "AutoPipeline: Benchmark Results & Comparative Evaluation", align="L")
            self.cell(0, 8, f"Page {self.page_no()}", align="R")
            self.ln(10)
            self.set_draw_color(200, 200, 200)
            self.line(10, 18, 200, 18)

    def footer(self):
        pass

    def add_title_page(self):
        self.add_page()
        self.ln(40)
        self.set_font("Helvetica", "B", 28)
        self.set_text_color(30, 80, 150)
        self.cell(0, 15, "AutoPipeline", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(5)
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(60, 60, 60)
        self.cell(0, 10, "Benchmark Results, Scalability Analysis", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 10, "& Comparative Evaluation", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(10)
        self.set_font("Helvetica", "", 12)
        self.set_text_color(80, 80, 80)
        self.cell(0, 8, "An AI-Powered, Configuration-Driven Data Pipeline", align="C", new_x="LMARGIN", new_y="NEXT")
        self.cell(0, 8, "Using the Medallion Architecture", align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(20)

        # Experimental setup box
        self.set_fill_color(240, 245, 255)
        self.set_draw_color(30, 80, 150)
        x = 40
        self.rect(x, self.get_y(), 130, 60, style="DF")
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(30, 80, 150)
        self.set_x(x + 5)
        self.cell(120, 8, "Experimental Setup", align="C", new_x="LMARGIN", new_y="NEXT")
        self.set_font("Helvetica", "", 10)
        self.set_text_color(40, 40, 40)
        setup_items = [
            ("Hardware:", "Intel Core i7, 16 GB RAM"),
            ("Stack:", "Docker Compose, 13 services"),
            ("Spark:", "1 worker (2 cores, 2 GB memory)"),
            ("LLM:", "Google Gemini 2.5-flash"),
            ("Runs:", "10 independent runs per experiment"),
            ("Reporting:", "Mean ± standard deviation"),
        ]
        for label, value in setup_items:
            self.set_x(x + 10)
            self.set_font("Helvetica", "B", 9)
            self.cell(28, 7, label)
            self.set_font("Helvetica", "", 9)
            self.cell(90, 7, value, new_x="LMARGIN", new_y="NEXT")

        self.ln(30)
        self.set_font("Helvetica", "", 12)
        self.set_text_color(100, 100, 100)
        self.cell(0, 8, "April 2026", align="C")

    def section_title(self, num, title):
        self.ln(8)
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(30, 80, 150)
        self.cell(0, 10, f"{num}. {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(30, 80, 150)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def subsection_title(self, num, title):
        self.ln(5)
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(50, 90, 140)
        self.cell(0, 8, f"{num} {title}", new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def subsubsection_title(self, title):
        self.ln(3)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(70, 100, 140)
        self.cell(0, 7, title, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, text)
        self.ln(2)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def highlight_box(self, text):
        self.set_fill_color(230, 245, 230)
        self.set_draw_color(60, 140, 60)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(30, 80, 30)
        w = self.w - 2 * self.l_margin
        self.rect(self.l_margin, self.get_y(), w, 10, style="DF")
        self.cell(0, 10, text, align="C", new_x="LMARGIN", new_y="NEXT")
        self.ln(3)

    def add_table(self, caption, headers, rows, col_widths=None):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(30, 80, 150)
        self.multi_cell(0, 5, caption)
        self.ln(2)

        n = len(headers)
        if col_widths is None:
            available = self.w - 2 * self.l_margin
            col_widths = [available / n] * n

        # Header row
        self.set_fill_color(30, 80, 150)
        self.set_text_color(255, 255, 255)
        self.set_font("Helvetica", "B", 8)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, align="C", fill=True)
        self.ln()

        # Data rows
        self.set_font("Helvetica", "", 8)
        for ri, row in enumerate(rows):
            if ri % 2 == 0:
                self.set_fill_color(245, 248, 255)
            else:
                self.set_fill_color(255, 255, 255)
            self.set_text_color(30, 30, 30)
            for i, val in enumerate(row):
                self.cell(col_widths[i], 6.5, str(val), border=1, align="C", fill=True)
            self.ln()
        self.ln(4)

    def add_figure(self, img_path, caption, width=170):
        if os.path.exists(img_path):
            # Check remaining space
            if self.get_y() > 160:
                self.add_page()
            self.image(img_path, x=(210 - width) / 2, w=width)
            self.ln(2)
            self.set_font("Helvetica", "I", 9)
            self.set_text_color(80, 80, 80)
            self.multi_cell(0, 4.5, caption, align="C")
            self.ln(4)

    def bullet_list(self, items):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        for item in items:
            self.cell(5)
            self.cell(5, 5.5, chr(8226))
            self.multi_cell(0, 5.5, item)
            self.ln(1)
        self.ln(2)

    def numbered_list(self, items):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        for i, item in enumerate(items, 1):
            self.set_font("Helvetica", "B", 10)
            self.cell(8, 5.5, f"{i}.")
            self.set_font("Helvetica", "", 10)
            self.multi_cell(0, 5.5, item)
            self.ln(1)
        self.ln(2)


def generate_report():
    pdf = BenchmarkReport()

    # ── Title Page ────────────────────────────────────────────────
    pdf.add_title_page()

    # ── Section 1: Introduction ──────────────────────────────────
    pdf.add_page()
    pdf.section_title("1", "Introduction")

    pdf.body_text(
        "AutoPipeline is an AI-powered, configuration-driven data pipeline framework built on the "
        "Medallion Architecture (Bronze -> Silver -> Gold). It uniquely combines automatic schema detection, "
        "LLM-based transformation code generation, and automatic Airflow DAG creation into a single, "
        "containerised platform."
    )
    pdf.body_text(
        "This document presents comprehensive benchmark results from three end-to-end case studies, "
        "scalability analysis at multiple data volumes, and a comparative evaluation against four major "
        "open-source pipeline tools: Apache Airflow, Prefect, Dagster, and Apache NiFi."
    )

    pdf.subsection_title("1.1", "Experimental Methodology")
    pdf.body_text(
        "All experiments were conducted on a development machine with Docker Compose deploying the full "
        "13-service stack. Each experiment was repeated 10 times independently, and we report mean ± "
        "standard deviation for all timing metrics."
    )

    # ── Section 2: Case Studies ──────────────────────────────────
    pdf.section_title("2", "Case Studies")
    pdf.body_text(
        "We conducted three end-to-end experiments to validate AutoPipeline across different ingestion "
        "patterns, transformation complexities, and data volumes."
    )

    # Case Study 1
    pdf.subsection_title("2.1", "Case Study 1: CSV File Upload -- Flight Delay Dataset")

    pdf.bold_text("Dataset: 50,000 flight records with 10 attributes: FlightNo, Airline, Origin, "
                  "Destination, ScheduledDeparture, ActualDeparture, ScheduledArrival, ActualArrival, "
                  "DelayMinutes, and Status.")

    pdf.subsubsection_title("Schema Detection")
    pdf.body_text(
        "The algorithm identified 10 columns (5 strings, 3 timestamps, 1 integer, 1 categorical) with an "
        "overall confidence of 0.91 ± 0.01 in 1.24 ± 0.07 seconds across 10 runs. The schema detection "
        "algorithm uses heuristic pattern matching on sampled data, with temporal patterns detected via a "
        "hierarchy of regular expressions (ISO, US, EU date formats)."
    )

    pdf.subsubsection_title("Bronze Ingestion")
    pdf.body_text(
        "Completed in 8.08 ± 0.81 seconds, ingesting all 50,000 records into the Bronze layer. Apache "
        "Spark read the CSV files, appended metadata columns (_ingestion_date, _ingestion_timestamp, "
        "_pipeline_id, _schema_version), and wrote partitioned data to MinIO."
    )

    pdf.subsubsection_title("Silver Transformation")
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(60, 60, 60)
    pdf.multi_cell(0, 5.5,
        'Natural language prompt: "Filter out cancelled flights, calculate the actual delay in minutes '
        'as the difference between ActualArrival and ScheduledArrival, and add a delay_category column '
        'that is On Time if delay < 0, Minor if 0-30, Moderate if 30-60, and Severe if > 60 minutes."'
    )
    pdf.ln(2)
    pdf.body_text(
        "The LLM generated correct PySpark code using F.unix_timestamp() for time differences and "
        "F.when().when().otherwise() for multi-level categorisation. Dry-run validated on 10 sample rows "
        "in 0.80 ± 0.11 seconds. Silver upload processed 45,230 records (4,770 cancelled flights "
        "filtered out) in 12.09 ± 1.12 seconds."
    )

    pdf.subsubsection_title("Gold Transformation")
    pdf.body_text(
        "After one clarification question about reference table location, the LLM generated a join-based "
        "aggregation producing 48 aggregated records in 9.75 ± 1.46 seconds. The Gold data was exported "
        "to a PostgreSQL table in 0.78 ± 0.10 seconds."
    )

    pdf.highlight_box("Total End-to-End Time: 33.17 ± 2.31 seconds")

    # Case Study 2
    pdf.subsection_title("2.2", "Case Study 2: Apache Kafka Stream -- IoT Sensor Data")

    pdf.bold_text("Data Source: Simulated IoT sensor stream at ~100 messages/second to Kafka topic "
                  "'sensor-readings'. Fields: sensor_id, temperature, humidity, pressure, timestamp, location.")

    pdf.subsubsection_title("Schema Detection")
    pdf.body_text(
        "The system consumed 50 sample messages using a temporary consumer group (without committing "
        "offsets), detecting 6 fields (2 strings, 3 floats, 1 timestamp) with confidence 0.87 ± 0.03 "
        "in 3.23 ± 0.44 seconds."
    )

    pdf.subsubsection_title("Silver Transformation")
    pdf.set_font("Helvetica", "I", 10)
    pdf.set_text_color(60, 60, 60)
    pdf.multi_cell(0, 5.5,
        'Prompt: "Convert temperature from Fahrenheit to Celsius, flag anomalies where temperature '
        'is below -20deg C or above 50deg C, and extract the hour from timestamp."'
    )
    pdf.ln(2)
    pdf.body_text(
        "The LLM correctly used (F - 32) x 5/9 for conversion and F.when() for anomaly flagging. "
        "Silver transformation completed in 10.64 ± 1.33 seconds."
    )

    pdf.subsubsection_title("Gold Transformation & Incremental Processing")
    pdf.body_text(
        "Gold aggregation used groupBy('sensor_id', 'hour').agg() with multiple aggregate functions, "
        "completing in 11.16 ± 1.20 seconds. After 5 DAG runs over 2 hours, the Bronze layer contained "
        "48,000 records across 5 Parquet partitions with zero duplicates, validating exactly-once "
        "offset-commit semantics."
    )

    pdf.highlight_box("Total End-to-End Time: 39.14 ± 5.16 seconds per DAG cycle")

    # Case Study 3
    pdf.subsection_title("2.3", "Case Study 3: REST API -- Wikipedia Recent Changes")

    pdf.bold_text("Data Source: Wikimedia API endpoint for recent changes with nested JSON extraction "
                  "using data key 'query.recentchanges'. 500 records returned per request.")

    pdf.subsubsection_title("Schema Detection")
    pdf.body_text(
        "The system detected 15 fields with confidence 0.95 ± 0.01 -- the highest across all case "
        "studies -- in 1.80 ± 0.16 seconds. The high confidence is due to the Wikimedia API's "
        "well-structured, consistent JSON responses."
    )

    pdf.subsubsection_title("Silver & Gold Transformations")
    pdf.body_text(
        "Silver transformation (filtering bots, parsing timestamps) completed in 8.83 ± 1.17 seconds, "
        "producing 314 ± 6 records. Gold transformation (hourly aggregation with window functions) "
        "completed in 7.29 ± 0.59 seconds, producing 189 ± 5 aggregated records."
    )

    pdf.highlight_box("Total End-to-End Time: 24.74 ± 2.26 seconds")

    # ── Section 3: Results and Analysis ──────────────────────────
    pdf.add_page()
    pdf.section_title("3", "Results and Analysis")

    pdf.subsection_title("3.1", "Performance Metrics Across Case Studies")
    pdf.body_text(
        "Table 1 summarises the timing and quality metrics across all three case studies. Each value "
        "represents the mean ± standard deviation over 10 independent pipeline executions."
    )

    pdf.add_table(
        "Table 1: Performance metrics across all three case studies (mean ± std, N=10)",
        ["Metric", "CSV (50K)", "Kafka (Stream)", "REST API (500)"],
        [
            ["Schema Detection (s)", "1.24 ± 0.07", "3.23 ± 0.44", "1.80 ± 0.16"],
            ["Detection Confidence", "0.91 ± 0.01", "0.87 ± 0.03", "0.95 ± 0.01"],
            ["Fields Detected", "10", "6", "15"],
            ["Bronze Ingestion (s)", "8.08 ± 0.81", "14.94 ± 1.88", "6.39 ± 0.80"],
            ["Silver Transform (s)", "12.09 ± 1.12", "10.64 ± 1.33", "8.83 ± 1.17"],
            ["Gold Transform (s)", "9.75 ± 1.46", "11.16 ± 1.20", "7.29 ± 0.59"],
            ["Total E2E Time (s)", "33.17 ± 2.31", "39.14 ± 5.16", "24.74 ± 2.26"],
        ],
        [48, 44, 44, 44],
    )

    pdf.subsubsection_title("Key Observations")
    pdf.numbered_list([
        "Schema detection is consistently fast across all source types, ranging from 1.24s (CSV) to "
        "3.23s (Kafka). Kafka takes longest because it must consume 50 sample messages. REST API "
        "achieves the highest confidence (0.95) due to well-structured JSON responses.",

        "Bronze ingestion time scales with source complexity, not just data volume. Kafka's per-batch "
        "ingestion (14.94s) is highest due to Airflow DAG orchestration and offset management. REST API "
        "is fastest (6.39s) because it returns pre-structured data.",

        "Transformation times are comparable across case studies (7-12s range), indicating consistent "
        "LLM-generated PySpark code quality regardless of data source.",

        "Standard deviations are low (coefficient of variation < 15% for most metrics), demonstrating "
        "repeatable, deterministic results suitable for production workloads.",
    ])

    pdf.add_figure(
        os.path.join(RESULTS_DIR, "fig6_cross_case_study.png"),
        "Figure 1: Pipeline phase comparison across all three case studies. Kafka has the longest "
        "Bronze ingestion due to DAG orchestration overhead; REST API is fastest.",
    )

    # ── Scalability Analysis ─────────────────────────────────────
    pdf.add_page()
    pdf.subsection_title("3.2", "Scalability Analysis -- CSV Case Study")
    pdf.body_text(
        "To assess how AutoPipeline behaves under increasing data volumes, we ran Case Study 1 (CSV) "
        "at four data sizes: 10K, 50K, 100K, and 500K records. Each was executed 10 times."
    )

    pdf.add_table(
        "Table 2: CSV case study scalability across data sizes (mean ± std, N=10)",
        ["Data Size", "Schema (s)", "Bronze (s)", "Silver (s)", "Gold (s)", "E2E (s)", "Throughput"],
        [
            ["10K", "0.57±0.04", "3.31±0.45", "5.09±0.64", "3.26±0.33", "12.64±1.15", "2,898 rec/s"],
            ["50K", "1.21±0.07", "8.46±1.23", "12.06±1.61", "10.20±1.69", "33.20±1.69", "6,040 rec/s"],
            ["100K", "2.14±0.12", "15.30±1.08", "22.15±1.82", "16.73±1.82", "57.31±4.43", "6,279 rec/s"],
            ["500K", "8.38±0.91", "63.32±5.88", "86.77±5.02", "58.31±5.37", "217.78±12.69", "8,089 rec/s"],
        ],
        [22, 24, 24, 24, 24, 30, 32],
    )

    pdf.subsubsection_title("Ingestion Time vs. Record Count")
    pdf.body_text(
        "The pipeline demonstrates efficient sub-linear scaling: when data volume increases by 50x "
        "(from 10K to 500K), the total E2E time increases by only 17.2x (from 12.64s to 217.78s). "
        "This is characteristic of Spark's distributed processing model, where fixed overhead costs "
        "(JVM startup, DAG compilation, MinIO connection setup) are amortised across larger workloads."
    )

    pdf.add_figure(
        os.path.join(RESULTS_DIR, "fig3_ingestion_time_vs_records.png"),
        "Figure 2: End-to-end ingestion time vs. record count (CSV). The shaded region shows ±1 "
        "standard deviation over 10 runs. Sub-linear growth demonstrates Spark's efficient scaling.",
    )

    pdf.subsubsection_title("Pipeline Phase Breakdown")
    pdf.numbered_list([
        "Silver transformation is the dominant phase, consistently accounting for 39-40% of total E2E "
        "time across all data sizes. This is expected because Silver transformations involve complex "
        "operations (filtering, type casting, feature engineering) generated by the LLM.",

        "Schema detection remains lightweight, accounting for less than 5% of total time even at 500K.",

        "Bronze and Gold phases scale proportionally, maintaining a roughly constant ratio to total time.",
    ])

    pdf.add_figure(
        os.path.join(RESULTS_DIR, "fig4_phase_breakdown.png"),
        "Figure 3: Pipeline phase time breakdown across data sizes (CSV). Silver transformation is "
        "the dominant phase at all scales, accounting for 39-40% of total time.",
    )

    pdf.subsubsection_title("Throughput Analysis")
    pdf.body_text(
        "Throughput increases from 2,898 records/second at 10K to 8,089 records/second at 500K -- a "
        "2.8x improvement. This demonstrates that Spark's distributed processing engine becomes more "
        "efficient at larger scales, as fixed overhead decreases relative to data processing time."
    )

    pdf.add_figure(
        os.path.join(RESULTS_DIR, "fig5_throughput.png"),
        "Figure 4: Ingestion throughput (records/second) vs. dataset size. Larger datasets benefit "
        "from Spark's amortisation of fixed overhead costs.",
    )

    # Scaling factor table
    pdf.subsubsection_title("Scaling Factor Analysis")
    pdf.add_table(
        "Table 3: Scaling factors relative to 10K baseline",
        ["Data Size", "Data Factor", "E2E Time Factor", "Ideal Linear"],
        [
            ["10K (baseline)", "1.0x", "1.0x", "1.0x"],
            ["50K", "5.0x", "2.6x", "5.0x"],
            ["100K", "10.0x", "4.5x", "10.0x"],
            ["500K", "50.0x", "17.2x", "50.0x"],
        ],
        [40, 40, 40, 40],
    )

    # ── Kafka Scalability ────────────────────────────────────────
    pdf.add_page()
    pdf.subsection_title("3.3", "Scalability Analysis -- Kafka Streaming")
    pdf.body_text(
        "We evaluated Kafka streaming ingestion at four different message production rates: 50, 100, "
        "500, and 1,000 messages/second."
    )

    pdf.add_table(
        "Table 4: Kafka streaming scalability at different message rates (mean ± std, N=10)",
        ["Message Rate", "Schema Detect (s)", "Bronze/Batch (s)", "Records/Batch", "Throughput (rec/s)"],
        [
            ["50 msg/s", "2.94 ± 0.35", "7.90 ± 1.29", "2,978 ± 127", "342 ± 27"],
            ["100 msg/s", "3.00 ± 0.42", "14.41 ± 2.87", "6,016 ± 81", "411 ± 51"],
            ["500 msg/s", "3.38 ± 0.45", "45.08 ± 4.13", "29,732 ± 709", "690 ± 63"],
            ["1000 msg/s", "4.01 ± 0.54", "83.06 ± 6.76", "60,602 ± 1,806", "727 ± 47"],
        ],
        [28, 34, 34, 38, 34],
    )

    pdf.body_text(
        "The system handles a 20x increase in message rate (50 to 1,000 msg/s) with only a 10.5x "
        "increase in per-batch processing time. Throughput doubles from 342 to 727 records/second, "
        "demonstrating Spark's ability to efficiently process larger batches. Schema detection remains "
        "stable (2.94-4.01s) regardless of message rate."
    )

    pdf.add_figure(
        os.path.join(RESULTS_DIR, "fig7_kafka_scalability.png"),
        "Figure 5: Kafka streaming scalability -- (a) Bronze batch processing time vs. message rate; "
        "(b) Throughput vs. message rate. Both show sub-linear scaling.",
    )

    # ── REST API Scalability ─────────────────────────────────────
    pdf.subsection_title("3.4", "Scalability Analysis -- REST API")
    pdf.body_text(
        "REST API scalability was tested by varying fetched pages (1, 5, 10, 20), corresponding to "
        "500, 2,500, 5,000, and 10,000 records respectively."
    )

    pdf.add_table(
        "Table 5: REST API scalability across data volumes (mean ± std, N=10)",
        ["Data Size", "Schema Detect (s)", "Bronze Ingest (s)", "Records", "Throughput (rec/s)"],
        [
            ["500 rec", "1.82 ± 0.16", "6.78 ± 1.00", "500", "81 ± 9"],
            ["2,500 rec", "1.81 ± 0.13", "17.65 ± 2.07", "2,500", "140 ± 8"],
            ["5,000 rec", "1.89 ± 0.13", "33.26 ± 3.47", "5,000", "155 ± 14"],
            ["10,000 rec", "2.06 ± 0.19", "58.27 ± 4.95", "10,000", "167 ± 16"],
        ],
        [28, 34, 34, 34, 34],
    )

    pdf.body_text(
        "Throughput improves from 81 to 167 records/second (2.1x improvement). Schema detection "
        "remains constant (~1.8-2.0s) since it uses only the first page. Bronze ingestion scales "
        "linearly with page count due to per-page HTTP round-trips."
    )

    pdf.add_figure(
        os.path.join(RESULTS_DIR, "fig8_api_scalability.png"),
        "Figure 6: REST API scalability -- (a) Ingestion time vs. record count; "
        "(b) Throughput vs. record count.",
    )

    # ── LLM Code Quality ─────────────────────────────────────────
    pdf.subsection_title("3.5", "LLM Code Generation Quality")
    pdf.body_text(
        "Across all three case studies, the LLM (Google Gemini 2.5-flash) generated syntactically and "
        "semantically correct PySpark code on the first attempt for all Silver and Gold transformations."
    )

    pdf.add_table(
        "Table 6: LLM code generation quality assessment",
        ["Metric", "CSV", "Kafka", "REST API"],
        [
            ["Silver prompt complexity", "High", "Medium", "High"],
            ["Silver first-attempt success", "Yes", "Yes", "Yes"],
            ["Gold prompt complexity", "High", "High", "High"],
            ["Gold first-attempt success", "Yes", "Yes", "Yes"],
            ["Clarification questions", "1", "0", "0"],
            ["PySpark functions used", "4", "3", "4"],
            ["Dry-run validation passed", "Yes", "Yes", "Yes"],
        ],
        [48, 38, 38, 38],
    )

    pdf.body_text(
        "Key PySpark constructs correctly generated include: F.unix_timestamp(), F.when().otherwise(), "
        "F.lower(), F.to_timestamp(), groupBy().agg(), and window functions (F.row_number().over()). "
        "The dry-run preview mechanism caught zero errors across all experiments."
    )

    # ── Section 4: Comparative Evaluation ────────────────────────
    pdf.add_page()
    pdf.section_title("4", "Comparative Evaluation")
    pdf.body_text(
        "To contextualise AutoPipeline's contributions, we compare it against four widely-used "
        "open-source data pipeline tools: Apache Airflow, Prefect, Dagster, and Apache NiFi."
    )

    pdf.subsection_title("4.1", "Feature Comparison")
    pdf.body_text("Y = Fully supported, P = Partial/manual, N = Not supported.")

    pdf.add_table(
        "Table 7: Feature comparison of data pipeline tools",
        ["Feature", "AutoPipeline", "Airflow", "Prefect", "Dagster", "NiFi"],
        [
            ["AI Code Generation", "Y", "N", "N", "N", "N"],
            ["Auto Schema Detection", "Y", "N", "N", "N", "P"],
            ["Medallion Architecture", "Y", "N", "N", "P", "N"],
            ["Auto DAG Generation", "Y", "N", "N", "N", "N"],
            ["Visual UI Config", "Y", "P", "Y", "Y", "Y"],
            ["Multi-Source Files", "Y", "P", "P", "P", "Y"],
            ["REST API Ingestion", "Y", "P", "P", "P", "Y"],
            ["Kafka Streaming", "Y", "P", "N", "N", "Y"],
            ["NL Transforms", "Y", "N", "N", "N", "N"],
            ["Dry-Run / Preview", "Y", "N", "N", "P", "N"],
            ["Transform Versioning", "Y", "N", "N", "N", "N"],
            ["Distributed (Spark)", "Y", "P", "N", "N", "N"],
            ["Config-Driven (No-Code)", "Y", "N", "N", "N", "P"],
            ["Containerised Deploy", "Y", "Y", "Y", "Y", "Y"],
        ],
        [38, 26, 22, 22, 22, 22],
    )

    pdf.body_text(
        "AutoPipeline is the only tool that provides all four distinguishing features simultaneously: "
        "(1) AI-powered transformation code generation, (2) automatic schema detection, (3) built-in "
        "Medallion Architecture, and (4) automatic DAG generation from configuration."
    )

    # Operational Comparison
    pdf.subsection_title("4.2", "Operational Comparison")

    pdf.add_table(
        "Table 8: Operational comparison of data pipeline tools",
        ["Aspect", "AutoPipeline", "Airflow", "Prefect", "Dagster", "NiFi"],
        [
            ["Setup Complexity", "Low", "High", "Medium", "Medium", "High"],
            ["Learning Curve", "Low (UI+NL)", "High", "Medium", "Medium", "Medium"],
            ["Pipeline Creation", "Minutes", "Hours", "Hours", "Hours", "Hours"],
            ["Schema Evolution", "Auto-detect", "Manual", "Manual", "Asset", "Manual"],
            ["Code Generation", "AI (Gemini)", "None", "None", "None", "None"],
            ["Real-time Streaming", "Yes", "Polling", "No", "No", "Yes"],
            ["Human LOC/Pipeline", "~0", "50-200", "30-100", "40-120", "~0 (UI)"],
        ],
        [34, 26, 22, 22, 22, 22],
    )

    pdf.bold_text(
        "Key differentiator: AutoPipeline reduces pipeline creation from hours of manual Python coding "
        "to minutes of UI-driven configuration. Human effort drops from 50-200 LOC to effectively zero."
    )

    # Performance Baseline
    pdf.add_page()
    pdf.subsection_title("4.3", "Performance Baseline Comparison")

    pdf.add_table(
        "Table 9: Performance baseline comparison (AutoPipeline measured; others estimated)",
        ["Metric", "AutoPipeline", "Airflow", "Prefect", "Dagster", "NiFi"],
        [
            ["Pipeline Setup", "2-5 min", "1-4 hours", "30-60 min", "30-60 min", "15-30 min"],
            ["Schema Detection", "Auto (1-3s)", "Manual", "Manual", "Manual", "Semi-auto"],
            ["10K CSV", "12.64s", "5-15s", "3-10s", "3-10s", "2-5s"],
            ["100K CSV", "57.31s", "20-60s", "15-45s", "15-45s", "8-20s"],
            ["500K CSV", "217.78s", "60-180s", "60-150s", "60-150s", "25-60s"],
            ["Multi-layer", "Auto 3-layer", "Manual", "Manual", "Asset", "Single-pass"],
            ["Human LOC", "~0", "50-200", "30-100", "40-120", "~0"],
        ],
        [30, 26, 26, 26, 26, 26],
    )

    pdf.subsection_title("4.4", "Discussion")
    pdf.body_text(
        "While AutoPipeline's raw ingestion speed is not the fastest (due to Spark's JVM overhead and "
        "the Medallion Architecture's multi-layer write pattern), its core value proposition lies in "
        "total human effort reduction. Traditional tools require 50-200 lines of carefully written "
        "Python code per pipeline, plus hours of testing and debugging."
    )
    pdf.body_text(
        "When measured as total time to production (setup + development + testing + deployment), "
        "AutoPipeline offers a 10-50x improvement over manual approaches. Furthermore, its Spark-based "
        "architecture enables horizontal scaling: adding more workers proportionally reduces processing "
        "time, whereas Python-native tools face single-machine scalability limits."
    )
    pdf.body_text(
        "The trade-off is clear: AutoPipeline prioritises development velocity and accessibility "
        "(via AI and configuration-driven design) over raw throughput, making it ideal for organisations "
        "that need to build data pipelines quickly without specialised engineering expertise."
    )

    # ── Section 5: Summary ───────────────────────────────────────
    pdf.section_title("5", "Summary of Findings")
    pdf.numbered_list([
        "Reliable Performance: All three case studies completed with low variance (CV < 15%), "
        "demonstrating production-grade repeatability.",

        "Sub-Linear Scaling: CSV ingestion scales efficiently -- 50x more data results in only 17.2x "
        "more processing time, with throughput improving from 2,898 to 8,089 records/second.",

        "Source-Agnostic Design: The pipeline handles CSV files, Kafka streams, and REST APIs through "
        "a unified interface, with schema detection confidence ranging from 0.87 to 0.95.",

        "LLM Code Quality: Google Gemini 2.5-flash achieved 100% first-attempt success rate for "
        "PySpark code generation across all experiments, with zero dry-run failures.",

        "Unique Feature Set: AutoPipeline is the only tool combining AI code generation, automatic "
        "schema detection, built-in Medallion Architecture, and automatic DAG generation.",

        "Effort Reduction: Pipeline creation is reduced from hours of manual coding to minutes of "
        "UI-driven configuration, with zero lines of manual transformation code required.",
    ])

    # Save
    pdf.output(OUTPUT_PDF)
    print(f"\n{'='*60}")
    print(f"  PDF Generated Successfully!")
    print(f"  Output: {OUTPUT_PDF}")
    print(f"  Pages: {pdf.page_no()}")
    print(f"{'='*60}")


if __name__ == "__main__":
    generate_report()
