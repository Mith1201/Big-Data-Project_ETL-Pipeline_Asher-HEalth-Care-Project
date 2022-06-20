import logging
import logging.config

logging.config.fileConfig('../util/logging_to_file.conf')
logger = logging.getLogger("validations")


def get_curr_date(spark):
    try:
        vdf = spark.sql(""" select current_date """)
        logger.info("Validate spark object by printing current date:" + str(vdf.collect()))
    except Exception as exp:
        logger.error("Error in method spark_curr_date().Please check the stack Trace." + str(exp), exc_info=True)
        raise
    else:
        logger.info("Spark object is validated. Spark object is ready")


def df_count(df, df_Name):
    try:
        logger.info(f"The dataframe validation by count df_count() is started for Dataframe {df_Name}")
        df_count = df.count()
        logger.info(f"The dataframe count is {df_count}")
    except Exception as exp:
        logger.error("Error in the method df_count(). Please check the stack trace" + str(exp), exc_info=True)
    else:
        logger.info("The dataframe validation by count df_count() is completed successfully")


def df_top_10(df, df_Name):
    try:
        logger.info(f"The dataframe validation by top10 record df_top_10 is atrted for dataframe {df_Name}")
        logger.info(f"The dataframe top 10 records are :")
        df_pandas = df.limit(10).toPandas()
        logger.info('\n \t' + df_pandas.to_string(index=False))
    except Exception as exp:
        logger.error("Error in method df_top_10.Please check the stack Trace" + str(exp), exc_info=True)
        raise
    else:
        logger.info("The dataframes validation of top 10 records completed...")


def df_print_schema(df, df_Name):
    try:
        logger.info(f"The Dataframe schema validation for DataFrame {df_Name}")
        sch = df.schema.fields
        logger.info(f"The dataframe {df_Name} schema is: ")
        for i in sch:
            logger.info(f"\t{i}")
    except Exception as exp:
        logger.error("Error in the method df_show_schema(). Please check the Stack Trace" + str(exp))
        raise
    else:
        logger.info("The Dataframe schema validation is completed..")
