from pyspark.sql.types import StructType, StructField, StringType, IntegerType

CUSTOMER_SCHEMA = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("ingestion_date", StringType(), True),
])

CLEANING_RULES = {
    "name": {
        "remove_special_chars": True,
        "keep_apostrophe": True,
        "keep_hyphen": True,
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
        "capitalize": True,
    },
    "city": {
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
        "capitalize": True,
    },
    "country": {
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
    },
    "phone": {
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
    },
}