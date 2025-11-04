from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, mean, min, max, stddev, round

def analyze_dataset(df_trip: DataFrame):
    """
    Етап аналізу даних:
    1. Отримати загальну інформацію про набір даних.
    2. Отримати базову статистику для числових стовпців.
    """

    print("=== 1️⃣ Загальна інформація про набір даних ===")

    # Кількість рядків і колонок
    num_rows = df_trip.count()
    num_cols = len(df_trip.columns)
    print(f"Кількість рядків: {num_rows}")
    print(f"Кількість колонок: {num_cols}\n")

    # Показати імена колонок
    print("Список колонок:")
    print(df_trip.columns, "\n")

    # Показати схему DataFrame
    print("Схема DataFrame:")
    df_trip.printSchema()

    # Перевірка на пропущені значення (NaN/null)
    print("\nКількість пропущених значень у кожній колонці:")
    df_trip.select([
        count(when(col(c).isNull(), c)).alias(c) for c in df_trip.columns
    ]).show(truncate=False)

    print("\n=== 2️⃣ Статистика по числових стовпцях ===")

    # Визначаємо числові колонки
    numeric_cols = [f.name for f in df_trip.schema.fields
                    if f.dataType.simpleString() in ("double", "long", "integer")]

    if not numeric_cols:
        print("Немає числових колонок у наборі даних.")
        return

    # Обчислення базової статистики
    stats_df = df_trip.select(
        *[
            round(mean(c), 2).alias(f"{c}_mean"),
            round(min(c), 2).alias(f"{c}_min"),
            round(max(c), 2).alias(f"{c}_max"),
            round(stddev(c), 2).alias(f"{c}_std")
        ]
        for c in numeric_cols
    )

    # Альтернатива (простішим способом)
    print("\nЗведена статистика (describe):")
    df_trip.select(numeric_cols).describe().show(truncate=False)

    print("\nАналіз числових колонок завершено ✅")

    return numeric_cols
