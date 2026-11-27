"""Validate the generated CSV files before they enter the pipeline.

Single-table rules are Pandera schemas; rules that need both files
(a change must point to a known license, and happen on or after its
creation) are plain functions run after the schemas.

Run: uv run python -m app.validate_csv
Exits with code 1 if any rule fails; details go to app/csv/validation.log.
"""
import os
import sys
from typing import Dict, List

import pandas as pd
import pandera.pandas as pa
from pandera.errors import SchemaErrors

from app.config import PROJECT_ROOT
from app.logger import logger, set_logging

LICENSE_TYPES = ["PASS", "SIM", "SUPERVISION"]
MIN_PRICE, MAX_PRICE = 0, 5000

# ----------------------------
# Single-table rules
# ----------------------------
INITIAL_LICENSES_SCHEMA = pa.DataFrameSchema(
    {
        "id": pa.Column(int, unique=True),                  # no duplicated ids
        "customer_id": pa.Column(int),
        "type": pa.Column(str, pa.Check.isin(LICENSE_TYPES)),
        "creation_date": pa.Column(pa.DateTime, coerce=True),
        "price": pa.Column(int, pa.Check.in_range(MIN_PRICE, MAX_PRICE)),
        "renewable": pa.Column(bool),
    },
    strict=True,  # unexpected columns are errors too
    name="initial_licenses",
)

LICENSE_CHANGES_SCHEMA = pa.DataFrameSchema(
    {
        "id": pa.Column(int, unique=True),
        "license_id": pa.Column(int),
        "date": pa.Column(pa.DateTime, coerce=True),
        "price": pa.Column(int, pa.Check.in_range(MIN_PRICE, MAX_PRICE)),
        "type": pa.Column(str, pa.Check.isin(LICENSE_TYPES)),
        "renewable": pa.Column(bool),
    },
    strict=True,
    name="license_changes",
)


# ----------------------------
# Cross-table rules: one boolean per change row, True = valid
# ----------------------------
def rule_license_exists(changes: pd.DataFrame, init: pd.DataFrame) -> pd.Series:
    # every change points to a license of initial_licenses.csv
    return changes["license_id"].isin(init["id"])


def rule_change_after_creation(changes: pd.DataFrame, init: pd.DataFrame) -> pd.Series:
    # no change before a license's creation date (unknown licenses are left to rule_license_exists)
    creation = changes["license_id"].map(init.set_index("id")["creation_date"])
    return creation.isna() | (changes["date"] >= creation)


CROSS_TABLE_RULES = {
    "license_exists": rule_license_exists,
    "change_after_creation": rule_change_after_creation,
}


# ----------------------------
# Validation
# ----------------------------
def check_schema(schema: pa.DataFrameSchema, df: pd.DataFrame) -> tuple:
    """Return (coerced dataframe or None, list of failures) for one schema."""
    try:
        return schema.validate(df, lazy=True), []  # lazy: collect every failure, not just the first
    except SchemaErrors as err:
        failures = []
        cases = err.failure_cases
        for (column, check), group in cases.groupby(["column", "check"], dropna=False, sort=False):
            failures.append({
                "rule": f"{schema.name}.{column if pd.notna(column) else '<table>'}: {check}",
                "count": len(group),
                "examples": group["failure_case"].astype(str).head(5).tolist(),
            })
        return None, failures


def validate(init: pd.DataFrame, changes: pd.DataFrame) -> List[Dict]:
    """Run all rules and return the failures (empty list = data is valid)."""
    init_ok, failures = check_schema(INITIAL_LICENSES_SCHEMA, init)
    changes_ok, changes_failures = check_schema(LICENSE_CHANGES_SCHEMA, changes)
    failures += changes_failures

    # cross-table rules need both tables with valid columns and parsed dates
    if init_ok is None or changes_ok is None:
        logger.warning("Schema errors found: cross-table rules skipped")
        return failures

    for name, rule in CROSS_TABLE_RULES.items():
        valid = rule(changes_ok, init_ok)
        if not valid.all():
            failures.append({
                "rule": f"cross_table.{name}",
                "count": int((~valid).sum()),
                "examples": changes_ok.loc[~valid, "id"].astype(str).head(5).tolist(),
            })
    return failures


def main(file_name_init_licenses: str, file_name_changed_licenses: str, folder_name: str = "csv") -> bool:
    """Validate the two CSV files, log the result and return True if they are valid."""
    folder = os.path.join(PROJECT_ROOT, folder_name)
    init = pd.read_csv(os.path.join(folder, file_name_init_licenses), index_col=False, header=0)
    changes = pd.read_csv(os.path.join(folder, file_name_changed_licenses), index_col=False, header=0)

    failures = validate(init, changes)
    for f in failures:
        logger.warning(f"Rule '{f['rule']}' failed for {f['count']} rows | examples: {f['examples']}")

    if failures:
        logger.warning(f"Validation FAILED: {len(failures)} rule(s) broken")
        return False
    logger.info(f"Validation passed: {len(init)} licenses, {len(changes)} changes")
    return True


if __name__ == '__main__':
    set_logging("validation.log")
    # files
    file_name_init_licenses = "initial_licenses.csv"
    file_name_changed_licenses = "license_changes.csv"
    sys.exit(0 if main(file_name_init_licenses, file_name_changed_licenses) else 1)
