import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.session import SparkSession
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
spark = SparkSession.builder.config('spark.serializer','org.apache.spark.serializer.KryoSerializer').config('spark.sql.hive.convertMetastoreParquet','false').config('sync-tool-classes','org.apache.hudi.aws.sync.AwsGlueCatalogSyncTool').getOrCreate()
sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={},
    connection_type="s3",
    format="parquet",
    connection_options={
        "paths": ["s3://hudi-final-demo-hb/raw/weather/delta_parquet/delta_gen.parquet"]
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
# ApplyMapping_node2 = ApplyMapping.apply(
#     frame=S3bucket_node1,
#     mappings=[
#         ("city_id", "string", "city_id", "string"),
#         ("date", "string", "date", "string"),
#         ("timestamp", "string", "timestamp", "string"),
#         ("relative_humidity", "decimal", "relative_humidity", "decimal"),
#         ("temperature", "decimal", "temperature", "decimal"),
#         ("absolute_humidity", "decimal", "absolute_humidity", "decimal"),
#     ],
#     transformation_ctx="ApplyMapping_node2",
# )

hudiwriteconfig = {
    "hoodie.datasource.write.operation":"upsert",
    'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
    "hoodie.datasource.write.recordkey.field": "city_id",
    "hoodie.datasource.write.recordkey.field": "timestamp",
    "hoodie.datasource.write.partitionpath.field": "date:SIMPLE",
    "hoodie.datasource.write.precombine.field": "timestamp",
    'hoodie.datasource.hive_sync.partition_fields': 'date'
}

hudidatabaseconfig = {
    "hoodie.table.name": "wether_data",
    "hoodie.datasource.hive_sync.table": "wether_data",
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.database": "wether_db",
    'hoodie.datasource.hive_sync.use_jdbc':'false',
    'hoodie.datasource.hive_sync.partition_extractor_class': 'org.apache.hudi.hive.MultiPartKeysValueExtractor',
    'hoodie.datasource.write.keygenerator.class': 'org.apache.hudi.keygen.CustomKeyGenerator'
}

hudiconfig = {
    'className' : 'org.apache.hudi',
    "connectionName": "hudi_connector",
    "path": "s3://hudi-final-demo-hb/final/weather/"
}

connectionconfig = {**hudiwriteconfig,**hudidatabaseconfig,**hudiconfig}
# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=S3bucket_node1,
    connection_type="marketplace.spark",
    connection_options=connectionconfig,
    transformation_ctx="S3bucket_node3",
)

job.commit()
