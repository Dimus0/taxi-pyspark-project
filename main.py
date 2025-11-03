import os
import sys
from pyspark.sql import SparkSession

sys.path.append(os.path.join(os.path.dirname(__file__),'funcions'))

try:
    from functions.extraction import run_extracion
    # from functions.transformation import run_transformation
    # from functions.analysis import run_analysis
    # from functions.recording import run_recording
except ImportError as e:
    print(f"Error imports module: {e}")
    sys.exit(1)


def main():
    
    
    INPUT_DATA_PATH = ''
    OUTPUT_RESULT_DIR = ''

    print("Iniziliazate SparkSession")
    spark = SparkSession.builder.appName("Taxi-pyspark-project").getOrCreate()

    try:
        print("\n Запуск Етапу Видобування.....")

        df_extracted = run_extracion(spark, INPUT_DATA_PATH)
        

        print("\n Запуск Етапу Трансформації.....")
    except Exception as e:
        print(f"Помилка виконання пайплайну: {e}")

        spark.stop()
        sys.exit(1)
    finally:

        spark.stop()
        print("\n Pipeline DONE!!")

if __name__ == "__main__":
    main()