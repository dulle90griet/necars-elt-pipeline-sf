import re
import pandas as pd

from snowflake.snowpark.functions import udf

@udf(name="split_description", input_types=["STRING"], output_type="ARRAY")
def split_description(description: str) -> list:
    door_search = re.search(r"\b(\ddr)\b", description.lower())
    door_start, door_end = door_search.span()

    vehicle_model = description[:door_start].strip()
    door_count = description[door_start:door_end]
    
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

def model(dbt, session):
    dbt.config(
        materialized = "table"
    )

    int_vehicle_df = dbt.ref("int_vehicle")

    description_lists = pd.Series(int_vehicle_df['vehicle_description'].apply(
        lambda x: split_description(x)
    ))
    description_cols_df = pd.DataFrame(
        description_lists.values.tolist(),
        columns=["model", "door_count", "transmission_type", "fuel_type"]
    ).replace("", None)

    dim_vehicle_df = int_vehicle_df.join(description_cols_df) \
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
