import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

sys.path.append(os.path.join(os.path.dirname(__file__),'funcions'))

try:
    from functions.extraction import run_extracion
    from functions.data_analysis import analyze_dataset
    from functions.business_qeustion import implementing_business_questions
    from functions.preprocessing import preprocess_for_ml
except ImportError as e:
    print(f"Error imports module: {e}")
    sys.exit(1)


def main():
    
    # Треба зробити щоб зчитувалося із зовнішньої дерикторії всі файли
    INPUT_DATA_PATH = '/app/data'
    OUTPUT_RESULT_DIR = ''

    print(f"\n {'='*15} Iniziliazate SparkSession {'='*15} \n")

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

        df_trip, df_dispatch_base, df_origin_base, df_vehicle,df_location = run_extracion(spark, INPUT_DATA_PATH)

        print("\n Запуск Етапу Трансформації.....")

        '''
        За коментовано для швидого преходу, якщо потрібно передивитися потрібно розкоментувати наступний рядок
            |
            |
            \/
        '''
        # analyze_data = analyze_dataset(df_trip)

        # implementing_business_questions(spark, df_trip, df_dispatch_base, df_origin_base, df_vehicle, df_location)

        print("\n Запуск preprocessing для ML...")
        
        df_ml = preprocess_for_ml(df_trip)

        df_ml.show(5)

    except Exception as e:
        print(f"\nПомилка виконання пайплайну: {e}")

        spark.stop()
        sys.exit(1)
    finally:

        spark.stop()
        print("\n Pipeline DONE!!")

if __name__ == "__main__":
    main()

