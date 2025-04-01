import re

import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F
from snowflake.snowpark import Window

def register_udf_split_descriptions():
    # dbt python models in Snowpark don't allow for named UDFs,
    # so we need to define a helper function which will be invoked by a lambda
    def description_splitter(description: str) -> list:
        stock_id, description = int(description[:5]), description[5:]

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
            
        return [stock_id, vehicle_model, door_count, transmission_type, fuel_type]

    # The anonymous UDF, with lambda call
    split_descriptions = F.udf(
        lambda x: description_splitter(x),
        input_types=[T.StringType()],
        return_type=T.ArrayType()
    )

    return split_descriptions

def model(dbt, session):
    dbt.config(
        materialized = "table"
    )

    int_vehicle_df = dbt.ref("int_vehicle")
    
    split_descriptions = register_udf_split_descriptions()
    
    # Snowpark makes joins on row numbers unworkable, so we first
    # need to prepend the stock_id to each description string
    descriptions_to_split = F.concat(
        int_vehicle_df.stock_id,
        int_vehicle_df.vehicle_description
    )
    # Using our UDF, generate a Snowpark Column in which each value 
    # is a list of description elements
    description_lists = split_descriptions(descriptions_to_split)

    # Explode those elements into separate columns in a Snowpark DF
    columns = ["stock_id", "model", "door_count", "transmission_type", "fuel_type"]
    description_cols_df = int_vehicle_df \
        .withColumn("description_lists_col", description_lists) \
        .select(
            *[F.col("description_lists_col")[i].alias(columns[i])
              for i in range(len(columns))]
        ).replace("", None)

    # Join these columns to the int_vehicle DF and generate dim_vehicle
    dim_vehicle_df = int_vehicle_df.join(description_cols_df, on="stock_id") \
        [[
            "stock_id",
            "stock_type",
            "make",
            "model",
            "door_count",
            "transmission_type",
            "fuel_type"
        ]]
    return dim_vehicle_df