from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("zarr-test").getOrCreate()

expression = (
    spark.read.format("zarr")
    .option("path", "/Users/antoine/Documents/GitHub/mcube-zarr/data/old_indications/TCGA_BRCA.zarr")
    .option("valuesNode", "transcriptomic/rnaseq_fpkm")
    .option("columnsNodes", "transcriptomic/genes")
    .option("indexNodes", "transcriptomic/records")
    .option("indexAliases", "sample")
    .option("columnAliases", "gene")
    .load()
)
expression.createOrReplaceTempView("expression")
expression.show(truncate=False)

variants = (
    spark.read.format("zarr")
    .option("path", "/Users/antoine/Documents/GitHub/mcube-zarr/data/old_indications/TCGA_BRCA.zarr")
    .option("valuesNode", "genomic/dnaseq_maf")
    .option("columnsNodes", "genomic/samples,genomic/variants")
    .option("indexNodes", "genomic/genes")
    .option("indexAliases", "gene")
    .option("columnAliases", "sample,variant")
    .load()
)
variants.createOrReplaceTempView("variants")
variants.show(truncate=False)

df = spark.sql("""
SELECT
  expression.sample,
  expression.gene,
  expression.value,
  to_json(
    map_from_entries(
      collect_list(named_struct('key', variants.variant, 'value', variants.value))
    )
  ) AS variants_json
FROM expression
INNER JOIN variants
  ON substring(expression.sample, 1, 16) = substring(variants.sample, 1, 16)
 AND expression.gene = variants.gene
WHERE variants.gene = 'TP53'
  AND variants.value <> 0
GROUP BY expression.sample, expression.gene, expression.value
""")

print("Schema:")
df.printSchema()
print("Sample rows:")
df.show(truncate=False)

spark.stop()
