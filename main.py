import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.append(os.path.join(os.path.dirname(__file__),'funcions'))

try:
    from functions.extraction import run_extracion
    from functions.data_analysis import analyze_dataset
    # from functions.transformation import run_transformation
    # from functions.analysis import run_analysis
    # from functions.recording import run_recording
except ImportError as e:
    print(f"Error imports module: {e}")
    sys.exit(1)


def main():
    
    # Треба зробити щоб зчитувалося із зовнішньої дерикторії всі файли
    INPUT_DATA_PATH = '/app/data'
    OUTPUT_RESULT_DIR = ''

    print("Iniziliazate SparkSession")

    # Парамери, щоб уникати outofmemory
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Taxi-pyspark-project") \
        .config("spark.driver.memory", "6g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    try:
        print("\n Запуск Етапу Видобування.....")

        df_trip, df_dispatch_base, df_origin_base, df_vehicle = run_extracion(spark, INPUT_DATA_PATH)

        analyze_data = analyze_dataset(df_trip)

        print("\n Запуск Етапу Трансформації..... В РОЗРОБЦІ")

    except Exception as e:
        print(f"Помилка виконання пайплайну: {e}")

        spark.stop()
        sys.exit(1)
    finally:

        spark.stop()
        print("\n Pipeline DONE!!")

if __name__ == "__main__":
    main()

