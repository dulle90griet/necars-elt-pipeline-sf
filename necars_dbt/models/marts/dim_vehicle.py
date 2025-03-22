import re

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

def register_udf_split_description():
    # dbt python models in Snowpark don't allow for named UDFs,
    # so we need to define a helper function to invoke in a lambda
    def description_splitter(description: str) -> list:
        door_search = re.search(r"\b(\d)dr\b", description.lower())
        door_start, door_end = door_search.span()

        vehicle_model = description[:door_start].strip()
        door_count = int(door_search.groups()[0])
        
        if (fuel_search := re.search(
            r"\b((?:(?:petrol|diesel|electric|plug-in|plug|in|hybrid)\s*)+)\b",
            description.lower()[door_end:]
        )):
            fuel_start = fuel_search.span()[0] + door_end
            fuel_end = fuel_search.span()[1] + door_end

            transmission_type = description[door_end:fuel_start].strip()
            fuel_type = description[fuel_start:fuel_end]
        else:
            transmission_type = description[door_end:].strip()
            fuel_type = None
            
        return [vehicle_model, door_count, transmission_type, fuel_type]

    # the anonymous UDF, with lambda call
    split_description = F.udf(
        lambda x: description_splitter(x),
        input_types=[T.StringType()],
        return_type=T.ArrayType()
    )

    return split_description

def model(dbt, session):
    dbt.config(
        materialized = "table"
    )

    int_vehicle_df = dbt.ref("int_vehicle")

    split_description = register_udf_split_description()
    
    # using our UDF, generate a Snowpark Column in which each value is a list
    # of description elements
    description_lists = split_description(int_vehicle_df["vehicle_description"])

    # explode those elements into separate columns in a Snowpark DF
    columns = ["model", "door_count", "transmission_type", "fuel_type"]
    description_cols_df = int_vehicle_df \
        .withColumn("description_lists_col", description_lists) \
        .select(
            *[F.col("description_lists_col")[i].alias(columns[i])
              for i in range(len(columns))]
        ).replace("", None)

    # join these columns to the int_vehicle DF and generate dim_vehicle
    # first, add row numbers - in Snowpark this requires assignment over a window
    # we don't want to change the order, so we use a constant ordering key
    window_spec = Window.orderBy(F.lit(1))
    int_vehicle_df = int_vehicle_df.with_column(
        "row_no",
        F.row_number().over(window_spec)
    )
    description_cols_df = description_cols_df.with_column(
        "row_no",
        F.row_number().over(window_spec)
    )
    # make the join and generate the dim_vehicle DF
    dim_vehicle_df = int_vehicle_df.join(
        description_cols_df,
        int_vehicle_df.col("row_no") == description_cols_df.col("row_no")
    )[[
        "stock_id",
        "stock_type",
        "make",
        "model",
        "door_count",
        "transmission_type",
        "fuel_type"
    ]]       
    
    return dim_vehicle_df