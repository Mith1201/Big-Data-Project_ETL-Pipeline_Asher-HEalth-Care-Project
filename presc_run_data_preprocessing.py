import logging.config
from pyspark.sql.functions import upper, lit, regexp_extract, col, concat_ws, count, when, isnan, avg, round, coalesce
from pyspark.sql.window import Window

logging.config.fileConfig("../util/logging_to_file.conf")
logger = logging.getLogger(__name__)


def perform_data_clean(df1, df2):
    ##clean df_city DataFrame:
    # select only required columns
    # apply upperfunctions for columns city,state,

    try:
        logger.info(f"perform_data_clean() is started")
        df_city_sel = df1.select(upper(df1.city).alias("city"),
                                 df1.state_id,
                                 upper(df1.state_name).alias("state_name"),
                                 upper(df1.county_name).alias("county_name"),
                                 df1.population,
                                 df1.zips)
        ###Clean df_fact DataFrame
        # 1 select only required columns
        # 2 Rename the columns
        logger.info(f" Data preprocessing started for Fact dataframe")
        df_fact_sel = df2.select(df2.npi.alias("presc_id"),
                                 df2.nppes_provider_last_org_name.alias("presc_lname"), \
                                 df2.nppes_provider_first_name.alias("presc_fname"),
                                 df2.nppes_provider_city.alias("presc_city"), \
                                 df2.nppes_provider_state.alias("presc_state"),
                                 df2.specialty_description.alias("presc_spclt"), \
                                 df2.years_of_exp, df2.drug_name,
                                 df2.total_claim_count.alias("trx_cnt"),
                                 df2.total_day_supply, \
                                 df2.total_drug_cost)
        # 3 Add a country field USA
        df_fact_sel = df_fact_sel.withColumn("country_name", lit("USA"))

        # 4 Clean years_of_exp field
        pattern = '\d+'
        idx = 0
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", regexp_extract(col("years_of_exp"), pattern, idx))

        # 5 Convert the years_of_exp datatype from string to number
        df_fact_sel = df_fact_sel.withColumn("years_of_exp", col("years_of_exp").cast("int"))

        # 6 combine first name and last name
        df_fact_sel = df_fact_sel.withColumn("presc_fullname", concat_ws(" ", "presc_fname", "presc_lname"))
        df_fact_sel = df_fact_sel.drop("presc_fname", "presc_lname")

        # 7 Check and clean all null//nan values
        #df_fact_sel.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df_fact_sel.columns]).show()

        # 8 Delete the records where presc_id is null

        df_fact_sel = df_fact_sel.dropna(subset="presc_id")

        # 9 Delete the records where drug_name is null
        df_fact_sel = df_fact_sel.dropna(subset="drug_name")

        # 10 Input TRX_CNT where it is null as avg of trx_cnt for prescriber
        spec = Window.partitionBy("presc_id")
        df_fact_sel = df_fact_sel.withColumn('trx_cnt', coalesce("trx_cnt", round(avg("trx_cnt").over(spec))))
        df_fact_sel = df_fact_sel.withColumn("trx_cnt",col("trx_cnt").cast("integer"))

    except Exception as exp:
        logger.error("Error in the method spark_curr_date(). Please check the Stack trace" + str(exp), exc_info=True)
    else:
        logger.info("perform data_clean() is complete")

    return df_city_sel, df_fact_sel
