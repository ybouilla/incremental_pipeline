from typing import Iterable

import pandas as pd
from app.logger import logger
import os

from app.config import PROJECT_ROOT



# rules
def rule_creation_date(merged):
    # Aucune modification avant la date de création d'une licence
    return merged["date"] >= merged["creation_date"]

def rule_non_duplicate_id(x):
    # Pas d'identifiants dupliqués
    return ~x["id"].duplicated()

def rule_id_exist(chgd, init):
    # Tous les license_id existent dans initial_licenses.csv
    return chgd["id"].isin(init["license_id"])

def rule_type_ok(x):
    # Les types sont valides ( PASS ,SIM , SUPERVISION )
    return x["type"].isin(["PASS", "SIM", "SUPERVISION"])

def rule_prices(x):
    # Les prix sont dans la plage attendue
    return (x["price"] >= 0) & (x["price"] <= 5000)

def main(file_name_init_licenses: str, file_name_changed_licenses: str, folder_name:str = "csv"):
    # logging configuration 
    dir_path = PROJECT_ROOT


    created_csv = pd.read_csv(os.path.join(dir_path, folder_name, file_name_init_licenses), index_col=False, header=0)
    changed_csv = pd.read_csv(os.path.join(dir_path, folder_name, file_name_changed_licenses), index_col=False, header=0)

    merged = pd.merge(changed_csv, created_csv, left_on="license_id", right_on="id", how="left")

    # rules

    # Les distributions des caractéristiques
    # check if there are less constumer than ids
    rule_distr_1 = lambda chgd, init: chgd["customer_id"].nunique() < init["id"].nunique()
    # check if renewable are all set to True in created_csv
    rule_distr_2 = lambda x : x["renew"] == True

    # check if there are any dates in the future

    ruleset = {
        "date_modif": (rule_creation_date, (merged,),),
        "non_duplicated_id": (rule_non_duplicate_id,(created_csv,),),
        "id_exists": (rule_id_exist, (created_csv, changed_csv,),),
        "type_ok_changed_csv": (rule_type_ok, (changed_csv,),),
        "type_ok_created_csv": (rule_type_ok, (created_csv,),),
        "price_ok_changed_csv": (rule_prices, (changed_csv,),),
        "price_ok_created_csv": (rule_prices, (created_csv,),),
    }

    logging_df = pd.DataFrame()
    logging_df["is_valid"] = True
    for rule_name, (rule, args) in ruleset.items():

        result = rule(*args)
        logging_df[f"{rule_name}_valid"] = result
        logging_df["is_valid"] &= result
        
        if isinstance(result, Iterable):
            # row level checks
            invalid_count = (~result).sum()

            if invalid_count > 0:
                logger.warning(
                    f"Rule '{rule_name}' failed for {invalid_count} rows "
                    f"({invalid_count/len(logging_df):.2%})"
                )
            else:
                logger.info(f"Rule '{rule_name}' passed")
        else:
            # columns level checks
            if result:
                logger.warning(
                    f"Rule '{rule_name}' failed)"
                )
            else:
                
                logger.info(f"Rule '{rule_name}' passed")

if __name__ == '__main__':
    # files
    file_name_init_licenses = "initial_licenses.csv"
    file_name_changed_licenses = "license_changes.csv"
    main(file_name_init_licenses, file_name_changed_licenses)
