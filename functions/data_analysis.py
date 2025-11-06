from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, mean, min, max, stddev, round,when

def analyze_dataset(df_trip: DataFrame):
    """
    Етап аналізу даних:
    1. Отримати загальну інформацію про набір даних.
    2. Отримати базову статистику для числових стовпців.
    """

    print("=== 1️⃣ Загальна інформація про набір даних ===")
    df_sample = df_trip.limit(10000)

    # Кількість рядків і колонок
    num_rows = df_sample.count()
    num_cols = len(df_sample.columns)
    print(f"Кількість рядків: {num_rows}")
    print(f"Кількість колонок: {num_cols}\n")

    # Показати імена колонок
    print("Список колонок:")
    print(df_sample.columns, "\n")

    # Показати схему DataFrame
    print("Схема DataFrame:")
    df_sample.printSchema()

    # Перевірка на пропущені значення (NaN/null)
    print("\nКількість пропущених значень у кожній колонці:")
    for c in df_sample.columns:
        nulls = df_sample.select(count(when(col(c).isNull(), 1)).alias("nulls")).collect()[0][0]
        print(f"{c}: {nulls}")

    print("\n=== 2️⃣ Статистика по числових стовпцях ===")

    # Визначаємо числові колонки
    numeric_cols = [f.name for f in df_sample.schema.fields
                    if f.dataType.simpleString() in ("double", "long", "integer")]

    if not numeric_cols:
        print("Немає числових колонок у наборі даних.")
        return

    # Обчислення базової статистики
    aggs = []

    for c in numeric_cols:
        aggs.append(round(mean(col(c)), 2).alias(f"{c}_mean"))
        aggs.append(round(min(col(c)), 2).alias(f"{c}_min"))
        aggs.append(round(max(col(c)), 2).alias(f"{c}_max"))
        aggs.append(round(stddev(col(c)), 2).alias(f"{c}_std"))

    stats_df = df_sample.agg(*aggs)

    print("\n === Зведена статистика по числових колонках ===")
    stats_df.show(truncate=False)

    # Альтернатива (простішим способом)
    print("\nЗведена статистика (describe):")
    df_sample.select(numeric_cols).describe().show(truncate=False)

    print("\nАналіз числових колонок завершено ✅")

    return numeric_cols
