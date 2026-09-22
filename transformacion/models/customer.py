from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

CUSTOMER_SCHEMA = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("date_joined", TimestampType(), True),
])

CLEANING_RULES = {
    "first_name": {
        "remove_special_chars": True,
        "keep_apostrophe": True,
        "keep_hyphen": True,
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
        "capitalize": True,
    },
    "last_name": {
        "remove_special_chars": True,
        "keep_apostrophe": True,
        "keep_hyphen": True,
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
        "capitalize": True,
    },
    "email": {
        "strip_whitespace": True,
        "lowercase": True,
        "null_replacement": "UNKNOWN",
    },
    "country": {
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
    },
    "city": {
        "strip_whitespace": True,
        "null_replacement": "UNKNOWN",
        "capitalize": True,
    },
}
