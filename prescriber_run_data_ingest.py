import logging
import logging.config

logging.config.fileConfig('../util/logging_to_file.conf')
logger = logging.getLogger("prescriber_run_data_ingest")


def load_files(spark, file_dir, file_format, header, inferSchema):
    try:
        logger.info("The load_files() function is started...")
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)

        elif file_format == 'csv':
            df = spark.read.format(file_format).options(header=header).options(inferSchema=inferSchema).load(file_dir)

    except Exception as exp:
        logger.error("Error in the method-load_file(). Please check the stack Trace." + str(exp), exc_info=True)
        raise
    else:
        logger.info(f"The input file {file_dir} is loaded to dataframe. The load_file completed successfully...")

    return df
