import re
import pandas as pd

def model(description: str) -> pd.DataFrame:
    if not (door_search := re.search(r"\b(\ddr)\b", description.lower())):
        return None
    
    vehicle_model = description[:door_search.span()[0]].strip()
    print(vehicle_model)
    vehicle_doors = door_search.groups()[0]
    print(vehicle_doors)

    if not (fuel_search := re.search(
        r"\b((?:(?:petrol|diesel|electric|plug-in|plug|in|hybrid)\s*)+)\b",
        description.lower()[door_search.span()[1] + 1:]
    )):
        return None
    vehicle_fuel_type = fuel_search.groups()[0]
    print(vehicle_fuel_type)

    vehicle_transmission_type = description[
        door_search.span()[1] : door_search.span()[1] + 1 + fuel_search.span()[0]
    ].strip()
    print(vehicle_transmission_type)

if __name__ == "__main__":
    import sys
    model(sys.argv[1])
