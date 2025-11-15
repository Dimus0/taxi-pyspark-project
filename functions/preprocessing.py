from pyspark.sql import DataFrame
from pyspark.sql.functions import col, hour, dayofweek, when, round as spark_round
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline


def preprocess_for_ml(df_trip: DataFrame):
    """
    Підготовка df_trip для машинного навчання з one-hot кодуванням hvfhs_license_num.
    Видаляє рядки з пропущеними датами та прибирає деякі числові колонки.
    Виконує нормалізацію числових фіч.
    """

    # ==========================
    # 1. Видалення дублікатів і аномалій
    # ==========================
    df = df_trip

    df = df.dropna(subset=[
        "request_datetime",
        "on_scene_datetime",
        "pickup_datetime",
        "dropoff_datetime"
    ])

    df = df.filter(
        (col("trip_miles") > 0) &
        (col("trip_time") > 0) &
        (col("driver_pay").isNotNull())
    )

    # ==========================
    # 2. Нові фічі
    # ==========================
    df = df.withColumn("pickup_hour", hour("pickup_datetime"))
    df = df.withColumn("pickup_dayofweek", dayofweek("pickup_datetime"))

    df = df.withColumn(
        "trip_duration_min",
        spark_round(col("trip_time") / 60.0, 2)
    )

    df = df.withColumn(
        "trip_speed_mph",
        spark_round(col("trip_miles") / (col("trip_time") / 3600.0), 2)
    )

    df = df.withColumn(
        "is_shared",
        when(
            (col("shared_request_flag") == "Y") |
            (col("shared_match_flag") == "Y"),
            1
        ).otherwise(0)
    )

    # ==========================
    # 3. Видалити зайві колонки
    # ==========================
    df = df.drop(
        "dispatching_base_num",
        "originating_base_num",
        "tolls",
        "congestion_surcharge",
        "tips"
    )

    # ==========================
    # 4. Заповнення NULL
    # ==========================
    df = df.fillna({
        "trip_speed_mph": 0.0
    })

    # ==========================
    # 5. Категоріальні колонки
    # ==========================
    categorical_cols = ["hvfhs_license_num"]

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in categorical_cols
    ]

    encoders = [
        OneHotEncoder(
            inputCols=[f"{c}_idx" for c in categorical_cols],
            outputCols=[f"{c}_vec" for c in categorical_cols]
        )
    ]

    # ==========================
    # 6. Числові фічі
    # ==========================
    numeric_cols = [
        "PULocationID",
        "DOLocationID",
        "trip_miles",
        "trip_time",
        "trip_duration_min",
        "pickup_hour",
        "pickup_dayofweek",
        "trip_speed_mph",
        "is_shared",
        "base_passenger_fare",
        "bcf",
        "sales_tax"
    ]

    # ==========================
    # 7. Vector Assembler
    # ==========================
    assembler = VectorAssembler(
        inputCols=numeric_cols + [f"{c}_vec" for c in categorical_cols],
        outputCol="features_raw"
    )

    # ==========================
    # 8. Стандартизація / нормалізація
    # ==========================
    scaler = StandardScaler(
        inputCol="features_raw",
        outputCol="features",
        withMean=True,   # центроване
        withStd=True     # масштабоване
    )

    # ==========================
    # 9. Pipeline
    # ==========================
    pipeline = Pipeline(stages=indexers + encoders + [assembler, scaler])

    model = pipeline.fit(df)
    final_df = model.transform(df)

    final_df = final_df.withColumnRenamed("driver_pay", "label")

    return final_df.select("features", "label")
