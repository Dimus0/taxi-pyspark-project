from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    year,
    count,
    col,
    avg,
    rank,
    hour,
    month,
    lag,
    to_timestamp,
    create_map,
    lit,
    when,
    sum,
    round,
    row_number,
    dayofweek,
)
from pyspark.sql.window import Window
import os


def implementing_business_questions(
        spark: SparkSession, df_trip, df_dispatch_base, df_origin_base, df_vehicle, df_location
):
    # 1
    # Filter + Join + Group By
    print("\n1 Бізнес-питання")
    top_5_PULocation = (
        df_trip.filter(col("trip_time") > 600)
        .filter(year("pickup_datetime") >= 2020)
        .join(
            df_location.withColumnRenamed("LocationID", "PU_id"),
            df_trip["PULocationID"] == col("PU_id"),
            "left",
        )
        .groupBy("Borough")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
        .limit(5)
    )

    print("\nТоп 5 Районів із яких роблять замовлення із 2020")
    top_5_PULocation.show()

    # 2
    # join + filter + group by
    print("\n2 Бізнес-питання")
    avg_travel_matrix = (
        df_trip.filter(col("trip_time") > 0)
        .join(
            df_location.withColumnRenamed("LocationID", "PU_id")
            .withColumnRenamed("Borough", "PU_Borough")
            .withColumnRenamed("Zone", "PU_Zone")
            .withColumnRenamed("service_zone", "PU_service_zone"),
            df_trip["PULocationID"] == col("PU_id"),
            "left",
        )
        .join(
            df_location.withColumnRenamed("LocationID", "DO_id")
            .withColumnRenamed("Borough", "DO_Borough")
            .withColumnRenamed("Zone", "DO_Zone")
            .withColumnRenamed("service_zone", "DO_service_zone"),
            df_trip["DOLocationID"] == col("DO_id"),
            "left",
        )
        .groupBy("PU_Borough", "DO_Borough")
        .agg(avg("trip_time").alias("avg_trip_time"))
    )

    print("\nСередній час поїздки між районами (де був зроблений заказ та куди було заплановано)")
    avg_travel_matrix.show()

    # 3
    print("\n3 Бізнес-питання")
    w = Window.orderBy(round(col("avg_fare"), 2).desc())

    base_rank = (
        df_trip.filter(col("base_passenger_fare") > 0)
        .groupBy("dispatching_base_num")
        .agg(avg("base_passenger_fare").alias("avg_fare"))
        .withColumn("rank", rank().over(w))
    )

    print("\nРейтинг диспетчерських баз за середнім доходом")
    base_rank.show()

    # 4
    print("\n4 Бізнес-питання")
    tips_rank = (
        df_trip.filter(col("tips") > 0)
        .groupBy("hvfhs_license_num")
        .agg(avg("tips").alias("avg_tips"))
        .orderBy(round(col("avg_tips"), 2).desc())
    )

    print("\nСердні чайові по компанії")
    tips_rank.show()

    # 5
    print("\n5 Бізнес-питання")
    w = Window.partitionBy("hvfhs_license_num").orderBy("year", "month")

    monthly_revenue = (
        df_trip.filter(col("base_passenger_fare") > 0)
        .withColumn("year", year("pickup_datetime"))
        .withColumn("month", month("pickup_datetime"))
        .groupBy("hvfhs_license_num", "year", "month")
        .agg(sum("base_passenger_fare").alias("monthly_income"))
        .withColumn("prev_month_income", lag("monthly_income").over(w))
        .withColumn("diff", col("monthly_income") - col("prev_month_income"))
        .withColumn("monthly_income", round(col("monthly_income"), 2))
        .withColumn("prev_month_income", round(col("prev_month_income"), 2))
        .withColumn("diff", round(col("diff"), 2))
        .orderBy("hvfhs_license_num", "year", "month")
    )

    print("\nМісячний дохід по ліцензіях (компанія таксі) + різниця з попереднім місяцем")
    monthly_revenue.show()

    # 6
    print("\n6 Бізнес-питання")
    longest_avg_trip = (
        df_trip.filter(col("trip_miles") > 0)
        .groupBy("dispatching_base_num")
        .agg(avg("trip_miles").alias("avg_trip_miles"))
        .orderBy(col("avg_trip_miles").desc())
    )

    print("\nДиспетчерські бази, які мають найдовші середні поїздки")
    longest_avg_trip.show(truncate=False)

    # 7
    print("\n7 Бізнес-питання")
    print("Визначити години доби з найбільшою кількістю поїздок")

    trips_by_hour = (
        df_trip.withColumn("hour", hour("pickup_datetime"))
        .groupBy("hour")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
        .limit(10)
    )

    trips_by_hour.show()

    # 8
    print("\n8 Бізнес-питання")
    print("Яка компанія має найбільшу частку коротких поїздок (<1 милі) у 2021 році, і скільки їх було")

    short_trip_share = (
        df_trip.filter((year("pickup_datetime") == 2021) & (col("trip_miles") > 0))
        .withColumn("is_short", when(col("trip_miles") < 1, 1).otherwise(0))
        .groupBy("hvfhs_license_num")
        .agg(
            sum("is_short").alias("short_trip_count"),
            count("*").alias("total_trips"),
            (sum("is_short") / count("*")).alias("short_trip_share"),
        )
        .orderBy(col("short_trip_share").desc())
    )
    short_trip_share.show()

    # 9
    print("\n9 Бізнес-питання")
    print("Топ-5 зон з найвищими середніми чайовими у 2022 році")

    tips_by_zone = (
        df_trip.filter((year("pickup_datetime") == 2022) & (col("tips") > 0))
        .join(
            df_location.withColumnRenamed("LocationID", "PU_id"),
            df_trip["PULocationID"] == col("PU_id"),
            "left",
        )
        .groupBy("Zone")
        .agg(avg("tips").alias("avg_tips"))
        .orderBy(col("avg_tips").desc())
        .limit(5)
    )
    tips_by_zone.show()

    # 10
    print("\n10 Бізнес-питання")
    print("Визначити компанію з найбільшим зростанням середнього доходу з місяця в місяць")

    w = Window.partitionBy("hvfhs_license_num").orderBy("year", "month")
    w2 = Window.partitionBy("year", "month").orderBy(col("growth").desc())

    fare_growth = (
        df_trip.filter(col("base_passenger_fare") > 0)
        .withColumn("year", year("pickup_datetime"))
        .withColumn("month", month("pickup_datetime"))
        .groupBy("hvfhs_license_num", "year", "month")
        .agg(avg("base_passenger_fare").alias("avg_fare"))
        .withColumn("prev_avg_fare", lag("avg_fare").over(w))
        .withColumn("growth", col("avg_fare") - col("prev_avg_fare"))
        .withColumn("rank", row_number().over(w2))
        .filter(col("rank") == 1)
        .drop("rank")
        .orderBy("year", "month")
        .limit(12)
    )

    fare_growth.show()

    # 11
    print("\n11 Бізнес-питання")
    print("Які бази мають найвищу середню оплату водіям, порівняно із середньою ціною поїздки")

    pay_ratio = (
        df_trip.filter((col("driver_pay") > 0) & (col("base_passenger_fare") > 0))
        .groupBy("dispatching_base_num")
        .agg(
            (avg("driver_pay") / avg("base_passenger_fare")).alias("driver_pay_ratio"),
        )
        .orderBy(col("driver_pay_ratio").desc())
        .limit(10)
    )
    pay_ratio.show()

    # 12
    print("\n12 Бізнес-питання (модифіковане)")
    print("Визначити топ-5 районів з найбільшою середньою тривалістю поїздок")

    borough_avg_duration = (
        df_trip.filter(col("trip_time") > 0)
        .join(
            df_location.withColumnRenamed("LocationID", "PU_id"),
            df_trip["PULocationID"] == col("PU_id"),
            "left",
        )
        .groupBy("Borough")
        .agg(avg("trip_time").alias("avg_trip_time"))
    )

    w = Window.orderBy(col("avg_trip_time").desc())

    top_boroughs = (
        borough_avg_duration.withColumn("rank", row_number().over(w))
        .filter(col("rank") <= 5)
        .drop("rank")
    )

    top_boroughs.show()

    # 13
    print("\n13 Бізнес-питання")
    print("Топ-5 зон посадки з найбільшою кількістю нічних поїздок (22:00–06:00)")

    night_trips_by_zone = (
        df_trip.withColumn("hour", hour("pickup_datetime"))
        .filter((col("hour") >= 22) | (col("hour") < 6))
        .join(
            df_location.withColumnRenamed("LocationID", "PU_id"),
            df_trip["PULocationID"] == col("PU_id"),
            "left",
        )
        .groupBy("Zone")
        .agg(count("*").alias("trip_count"))
        .orderBy(col("trip_count").desc())
        .limit(5)
    )

    night_trips_by_zone.show(truncate=False)

    # 14
    print("\n14 Бізнес-питання")
    print("Топ-3 компанії за середнім доходом з поїздки у вихідні дні")

    weekend_revenue = (
        df_trip.withColumn("dow", dayofweek("pickup_datetime"))  # 1 = неділя, 7 = субота
        .filter((col("dow") == 1) | (col("dow") == 7))
        .filter(col("base_passenger_fare") > 0)
        .groupBy("hvfhs_license_num")
        .agg(avg("base_passenger_fare").alias("avg_weekend_fare"))
        .orderBy(col("avg_weekend_fare").desc())
        .limit(3)
    )

    weekend_revenue.show(truncate=False)

    # 15
    print("\n15 Бізнес-питання")
    print("Топ-2 години доби для кожної компанії за середнім розміром чайових")

    tips_by_company_hour = (
        df_trip.filter(col("tips") > 0)
        .withColumn("hour", hour("pickup_datetime"))
        .groupBy("hvfhs_license_num", "hour")
        .agg(avg("tips").alias("avg_tips"))
    )

    w = Window.partitionBy("hvfhs_license_num").orderBy(col("avg_tips").desc())

    top_tipping_hours = (
        tips_by_company_hour.withColumn("rank", row_number().over(w))
        .filter(col("rank") <= 2)
        .orderBy("hvfhs_license_num", "rank")
    )

    top_tipping_hours.show(truncate=False)

    # 16
    print("\n16 Бізнес-питання")
    print("Зміна середньої тривалості поїздки по роках для кожної компанії та рік з найбільшим приростом")

    company_year_duration = (
        df_trip.filter(col("trip_time") > 0)
        .withColumn("year", year("pickup_datetime"))
        .groupBy("hvfhs_license_num", "year")
        .agg(avg("trip_time").alias("avg_trip_time"))
    )

    w = Window.partitionBy("hvfhs_license_num").orderBy("year")

    company_year_duration = (
        company_year_duration.withColumn(
            "prev_avg_trip_time", lag("avg_trip_time").over(w)
        ).withColumn("diff", col("avg_trip_time") - col("prev_avg_trip_time"))
    )

    w2 = Window.partitionBy("hvfhs_license_num").orderBy(col("diff").desc())

    max_growth_by_company = (
        company_year_duration.withColumn("rank", row_number().over(w2))
        .filter(col("rank") == 1)
        .orderBy(col("diff").desc())
    )

    max_growth_by_company.show(truncate=False)

    # 17
    print("\n17 Бізнес-питання")
    print("Топ-10 зон висадки з найвищим середнім доходом з довгих поїздок (> 2 милі)")

    long_trips_by_do_zone = (
        df_trip.filter(col("trip_miles") > 2)
        .filter(col("base_passenger_fare") > 0)
        .join(
            df_location.withColumnRenamed("LocationID", "DO_id"),
            df_trip["DOLocationID"] == col("DO_id"),
            "left",
        )
        .groupBy("Zone")
        .agg(avg("base_passenger_fare").alias("avg_long_trip_fare"))
        .orderBy(col("avg_long_trip_fare").desc())
        .limit(10)
    )

    long_trips_by_do_zone.show(truncate=False)

    # 18
    print("\n18 Бізнес-питання")
    print("Рейтинг компаній за часткою поїздок з чайовими")

    tips_share_by_company = (
        df_trip.withColumn("has_tip", when(col("tips") > 0, 1).otherwise(0))
        .groupBy("hvfhs_license_num")
        .agg(
            sum("has_tip").alias("trips_with_tips"),
            count("*").alias("total_trips"),
            (sum("has_tip") / count("*")).alias("tips_share"),
        )
        .orderBy(col("tips_share").desc())
    )

    tips_share_by_company.show(truncate=False)
