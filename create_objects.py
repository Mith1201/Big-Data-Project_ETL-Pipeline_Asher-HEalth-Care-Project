from pyspark.sql import SparkSession

import logging.config

logging.config.fileConfig(fname='../util/logging_to_file.conf')
logger=logging.getLogger("create_objects")
def get_spark_object(envn, appName):
    try:
        logger.info(f"get spark_object() is started. The '{envn}' envn is used")
        if envn == 'TEST':
            master = 'local'
        else:
            master = 'yarn'

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()
    except Exception as exp:
        logger.error("Error in get_spark_object.Please check the stack trace" + str(exp),exc_info=True)
        raise
    else:
        logger.info("Spark object created successfully")

    return spark
