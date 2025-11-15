from os import path
from pyspark.sql.types import DateType, StructType, StructField, StringType, LongType, DoubleType, IntegerType, \
    TimestampType,BinaryType
from pyspark.sql.functions import col,create_map,lit,when
from pyspark.sql import SparkSession
from pathlib import Path

"""
    Етап видобування
"""

list_name_base = {
    "B02914": "VULCAN CARS LLC",
    "B02907": "SABO ONE LLC",
    "B02908": "SABO TWO LLC",
    "B03035": "OMAHA LLC",
    "B02510": "TRI-CITY, LLC",
    "B02844": "ENDOR CAR & DRIVER, LLC",
    "B02877": "ZWOLF-NY, LLC",
    "B02866": "ZWEI-NY, LLC",
    "B02882": "ZWANZIG-NY, LLC",
    "B02869": "ZEHN-NY, LLC",
    "B02617": "WEITER LLC",
    "B02876": "VIERZEHN-NY, LLC",
    "B02865": "VIER-NY, LLC",
    "B02512": "UNTER LLC",
    "B02888": "SIEBZEHN-NY, LLC",
    "B02864": "SIEBEN-NY, LLC",
    "B02883": "SECHZEHN-NY, LLC",
    "B02875": "SECHS-NY, LLC",
    "B02682": "SCHMECKEN LLC",
    "B02880": "NEUNZEHN-NY, LLC",
    "B02870": "NEUN-NY, LLC",
    "B02404": "KUCHEN, LLC",
    "B02598": "HINTER LLC",
    "B02765": "GRUN LLC",
    "B02879": "FUNFZEHN-NY, LLC",
    "B02867": "FUNF-NY, LLC",
    "B02878": "ELF-NY, LLC",
    "B02887": "EINUNDZWANZIG-NY, LLC",
    "B02872": "EINS-NY, LLC",
    "B02836": "DRINNEN-NY LLC",
    "B02884": "DREIZEHN-NY, LLC",
    "B02835": "DREIST NY LLC",
    "B02764": "DANACH-NY, LLC",
    "B02889": "ACHTZEHN-NY, LLC",
    "B02871": "ACHT-NY, LLC",
    "B02395": "ABATAR LLC",
    "B03136": "GREENPOINT TRANSIT LLC ",
    "B02800": "FLATIRON TRANSIT LLC "
}

trip_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True),
    StructField("dispatching_base_num", StringType(), True),
    StructField("originating_base_num", StringType(), True),
    StructField("request_datetime", TimestampType(), True),
    StructField("on_scene_datetime", TimestampType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("PULocationID",LongType(),True),
    StructField("DOLocationID",LongType(),True),
    StructField("trip_miles", DoubleType(), True),
    StructField("trip_time", LongType(), True),
    StructField("base_passenger_fare", DoubleType(), True),
    # StructField("tolls", DoubleType(), True),
    StructField("bcf", DoubleType(), True),
    StructField("sales_tax", DoubleType(), True),
    # StructField("congestion_surcharge", DoubleType(), True),
    # StructField("airport_fee", LongType(), True), NULL value in original table
    StructField("tips", DoubleType(), True),
    StructField("driver_pay", DoubleType(), True),
    StructField("shared_request_flag", StringType(), True),
    StructField("shared_match_flag", StringType(), True),
    StructField("access_a_ride_flag", StringType(), True),
    StructField("wav_request_flag", StringType(), True),
    # StructField("wav_match_flag", BinaryType(), True) NULL value in original table
])

dispatch_base_schema = StructType([
    StructField("dispatch_base_num", StringType(), True)
])

# Таблиця бази, де поїздка починається
origin_base_schema = StructType([
    StructField("origin_base_num", StringType(), True)
])

'''
    HV0002 - Juno
    HV0003 - Uber
    HV0004 - Via
    HV0005 - Lyft
'''
vehicle_schema = StructType([
    StructField("hvfhs_license_num", StringType(), True)
])

location_dim_schema = StructType([
    StructField("LocationID", LongType(), True),
    StructField("Borough", StringType(), True),
    StructField("Zone", StringType(), True),
    StructField("service_zone", StringType(), True)
])


def run_extracion(spark: SparkSession, input_path: str):
    
    df_location_dim = get_location_dim(spark, input_path)
    
    path = Path(input_path)
    if not path.exists() or not path.is_dir():
        raise FileNotFoundError(f"Директорія не знайдена: {input_path}")

    parquet_files = sorted(path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"У директорії {input_path} немає parquet файлів")

    print(f"Знайдено {len(parquet_files)} parquet файлів. Починаємо зчитування...")

    # Зчитування csv файлу.
    df_trip = spark.read.schema(trip_schema).parquet(*[str(f) for f in parquet_files])


    # Обмеження даних з кожного parquet file по 10к з кожнго це буде 460к
    limit_per_file = 10000

    limited = []
    for i,f in enumerate(parquet_files, start=1):
        part_of_df = (
            spark.read.schema(trip_schema).parquet(str(f)).limit(limit_per_file)
        )
        limited.append(part_of_df)

    df_trip = limited[0]
    for df in limited[1:]:
        df_trip = df_trip.unionByName(df, allowMissingColumns=True)


    # print(f"Після об’єднання рядків: {df_trip.count()}, колонок: {len(df_trip.columns)}")
    df_trip = replacment_names_in_columns(df_trip)

    df_dispatch_base = df_trip.selectExpr("dispatching_base_num as dispatch_base_num").distinct()
    df_origin_base = df_trip.selectExpr("originating_base_num as origin_base_num").distinct()

    df_vehicle = df_trip.select("hvfhs_license_num").distinct()


    '''
        Закоментовано для швидкого переходу до іншого етапу, у разі потреба розкометувати для відображення інофрмації
    '''
    # print("DataFrames успішно створені:")
    # print(f" - df_trip: {df_trip.count()} рядків")
    # print(f" - df_dispatch_base: {df_dispatch_base.count()} унікальних dispatching_base_num")
    # print(f" - df_origin_base: {df_origin_base.count()} унікальних originating_base_num")
    # print(f" - df_vehicle: {df_vehicle.count()} унікальних hvfhs_license_num")
    # print(f" - df_locations: {df_location_dim.count()} rows")

    # Перевірка DataFrame
    # dataframe_verification(df_trip,df_dispatch_base,df_origin_base,df_vehicle,df_location)

    return df_trip, df_dispatch_base, df_origin_base, df_vehicle, df_location_dim

def get_location_dim(spark: SparkSession, input_path: str):

    path = Path(input_path)

    csv_files = list(path.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError("❌ CSV файл з зонами не знайдено!")

    print(f"\n Знайдено CSV файл: {csv_files[0].name}")

    df_locations = (
        spark.read
        .option("header", True)
        .schema(location_dim_schema)
        .csv(str(csv_files[0]))
        .distinct()
    )

    print("\n DIM таблиця локацій створена:")
    # df_locations.show(5)

    return df_locations

def dataframe_verification(df_trip,df_dispatch_base,df_origin_base,df_vehicle,df_location):
    print("\nПеревірка DataFrames:")
    print("df_trip:")
    df_trip.select("hvfhs_license_num", "pickup_datetime", "trip_miles").show(5, truncate=False)
    print("df_dispatch_base:")
    df_dispatch_base.show(5, truncate=False)
    print("df_origin_base:")
    df_origin_base.show(5, truncate=False)
    print("df_vehicle:")
    df_vehicle.show(5, truncate=False)
    print("df_location")
    df_location.show(5,truncate=False)

    print("Перевірка типів колонок:")
    df_trip.printSchema()
    df_dispatch_base.printSchema()
    df_origin_base.printSchema()
    df_vehicle.printSchema()
    df_location.printSchema()

def replacment_names_in_columns(df_trip):
    mapping_expr = create_map([lit(x) for pair in list_name_base.items() for x in pair])

    df_trip = (
        df_trip
        .withColumn("dispatching_base_num",mapping_expr[col("dispatching_base_num")])
        .withColumn("originating_base_num",mapping_expr[col("originating_base_num")])
    )

    df_trip = (
        df_trip
        .withColumn(
            "hvfhs_license_num",
            when(col("hvfhs_license_num") == "HV0002", "Juno")
            .when(col("hvfhs_license_num") == "HV0003", "Uber")
            .when(col("hvfhs_license_num") == "HV0004", "Via")
            .when(col("hvfhs_license_num") == "HV0005", "Lyft")
            .otherwise(col("hvfhs_license_num"))
        )
    )

    return df_trip