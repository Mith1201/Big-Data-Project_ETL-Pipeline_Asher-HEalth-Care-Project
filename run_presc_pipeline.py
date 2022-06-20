### Import all necessary Modules
import os
import sys

import get_all_variables as gav
from create_objects import get_spark_object
from validations import get_curr_date, df_count, df_top_10, df_print_schema
from prescriber_run_data_ingest import load_files
from presc_run_data_preprocessing import perform_data_clean
from presc_run_data_transform import city_report, top_5_prescribers
import logging
import logging.config

# load the logging config file
logging.config.fileConfig(fname='../util/logging_to_file.conf')


def main():
    ### Get Spark Object
    try:
        logging.info("main() is started...")
        spark = get_spark_object(gav.envn, gav.appName)
        logging.info("Spark object is created...")

        ### Validate Spark Object
        get_curr_date(spark)

        ### Initiate run_presc_data_ingest Script

        # load city file
        for file in os.listdir(gav.staging_dim_city):
            print("File is " + file)
            file_dir = gav.staging_dim_city + '\\' + file
            print(file_dir)

            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema

            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_city = load_files(spark, file_dir, file_format, header, inferSchema)

        ## Validate  run_data_ingest script for city dimension dataframe

        # load prescriber fact file
        for file in os.listdir(gav.staging_fact):
            print("File is" + file)
            file_dir = gav.staging_fact + '\\' + file
            print(file_dir)

            if file.split('.')[1] == 'csv':
                file_format = 'csv'
                header = gav.header
                inferSchema = gav.inferSchema
            elif file.split('.')[1] == 'parquet':
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'

        df_fact = load_files(spark, file_dir, file_format, header, inferSchema)
        # Validate dimension_city
        # df_count(df_city, "df_city")
        # df_top_10(df_city, "df_city")
        # Validate fact
        # df_count(df_fact, "df_fact")
        # df_top_10(df_fact, "df_fact")

        # Initiate presc_run_data_preprocessing script
        ## perform data cleaning

        df_city_sel, df_fact_sel = perform_data_clean(df_city, df_fact)

        # validate df_city_sel

        # df_top_10(df_city_sel, 'df_city_sel')
        # df_top_10(df_fact_sel, 'df_fact_sel')

        # Validation
        df_print_schema(df_fact_sel, 'df_fact_sel')

        ### Initiate presc_run_data_transform Script

        # Develop City Report and Prescriber Report
        df_city_final = city_report(df_city_sel, df_fact_sel)
        df_presc_final = top_5_prescribers(df_fact_sel)
        # Validate
        df_top_10(df_city_final, 'df_city_final')
        df_print_schema(df_city_final, 'df_city_final')

        #validate prescriber
        df_top_10(df_presc_final, 'df_presc_final')
        df_print_schema(df_presc_final, 'df_presc_final')
        # Setup error handling
        # Setup logging configuration mechanism

        logging.info("run_pres_pipeline.py is completed")
    except Exception as exp:
        logging.error("Error in the method-spark_curr_date(). Please check the Stack Trace" + str(exp), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    logging.info("run_presc_pipeline is started...")
    main()
