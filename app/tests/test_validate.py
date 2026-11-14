import os
from unittest.mock import MagicMock

import pandas as pd
import pytest

import app.validate_csv as m
from app.validate_csv import (
    INITIAL_LICENSES_SCHEMA,
    LICENSE_CHANGES_SCHEMA,
    check_schema,
    rule_change_after_creation,
    rule_license_exists,
    validate,
)


def init_df(**overrides):
    df = pd.DataFrame({
        "id": [1, 2, 3],
        "customer_id": [10, 11, 10],
        "type": ["PASS", "SIM", "SUPERVISION"],
        "creation_date": ["2024-01-01", "2024-01-05", "2024-02-01"],
        "price": [100, 1000, 5000],
        "renewable": [True, True, True],
    })
    for col, values in overrides.items():
        df[col] = values
    return df


def changes_df(**overrides):
    df = pd.DataFrame({
        "id": [0, 1, 2],
        "license_id": [1, 2, 3],
        "date": ["2024-01-10", "2024-01-05", "2024-03-01"],
        "price": [1000, 100, 5000],
        "type": ["SIM", "PASS", "SUPERVISION"],
        "renewable": [True, False, True],
    })
    for col, values in overrides.items():
        df[col] = values
    return df


def broken_rules(failures):
    return {f["rule"] for f in failures}


class TestSchemas:
    def test_01_valid_data_passes(self):
        assert validate(init_df(), changes_df()) == []

    def test_02_duplicated_id(self):
        _, failures = check_schema(INITIAL_LICENSES_SCHEMA, init_df(id=[1, 2, 2]))
        assert any(r.startswith("initial_licenses.id:") for r in broken_rules(failures))

    def test_03_invalid_type(self):
        _, failures = check_schema(LICENSE_CHANGES_SCHEMA, changes_df(type=["SIM", "GOLD", "PASS"]))
        assert failures[0]["rule"].startswith("license_changes.type: isin")
        assert failures[0]["examples"] == ["GOLD"]

    def test_04_price_out_of_range(self):
        _, failures = check_schema(INITIAL_LICENSES_SCHEMA, init_df(price=[-10, 2500, 6000]))
        assert failures[0]["rule"] == "initial_licenses.price: in_range(0, 5000)"
        assert failures[0]["count"] == 2

    def test_05_unparseable_date(self):
        _, failures = check_schema(LICENSE_CHANGES_SCHEMA, changes_df(date=["2024-01-10", "not a date", "2024-03-01"]))
        assert any(r.startswith("license_changes.date:") for r in broken_rules(failures))

    def test_06_unexpected_or_missing_column(self):
        _, failures = check_schema(INITIAL_LICENSES_SCHEMA, init_df().rename(columns={"renewable": "renew"}))
        assert failures  # strict schema: renamed column is both unexpected and missing

    def test_07_all_failures_reported_at_once(self):
        failures = validate(init_df(id=[1, 1, 3], price=[100, 1000, 9999]), changes_df())
        assert len(failures) == 2


class TestCrossTableRules:
    @staticmethod
    def parsed(init, changes):
        return INITIAL_LICENSES_SCHEMA.validate(init), LICENSE_CHANGES_SCHEMA.validate(changes)

    def test_01_license_exists(self):
        init, changes = self.parsed(init_df(), changes_df(license_id=[1, 4, 3]))
        assert rule_license_exists(changes, init).tolist() == [True, False, True]

    def test_02_change_after_creation(self):
        # license 2 created 2024-01-05: a change on 2024-01-04 is invalid, on the same day is valid
        init, changes = self.parsed(init_df(), changes_df(date=["2024-01-10", "2024-01-04", "2024-02-01"]))
        assert rule_change_after_creation(changes, init).tolist() == [True, False, True]

    def test_03_unknown_license_not_reported_twice(self):
        init, changes = self.parsed(init_df(), changes_df(license_id=[1, 99, 3]))
        assert rule_change_after_creation(changes, init).all()
        assert broken_rules(validate(init_df(), changes_df(license_id=[1, 99, 3]))) == {"cross_table.license_exists"}

    def test_04_skipped_when_schema_fails(self):
        failures = validate(init_df(type=["PASS", "GOLD", "SIM"]), changes_df(license_id=[1, 99, 3]))
        assert broken_rules(failures) == {"initial_licenses.type: isin(['PASS', 'SIM', 'SUPERVISION'])"}


class TestMain:
    @pytest.fixture
    def folder(self, tmp_path):
        return str(tmp_path)

    @pytest.fixture(autouse=True)
    def patch_logging(self, monkeypatch):
        self.mock_info = MagicMock()
        self.mock_warning = MagicMock()
        monkeypatch.setattr(m.logger, "info", self.mock_info)
        monkeypatch.setattr(m.logger, "warning", self.mock_warning)

    def write(self, folder, init, changes):
        init.to_csv(os.path.join(folder, "init.csv"), index=False)
        changes.to_csv(os.path.join(folder, "changed.csv"), index=False)

    def test_01_valid_files(self, folder):
        self.write(folder, init_df(), changes_df())
        assert m.main("init.csv", "changed.csv", folder) is True
        self.mock_warning.assert_not_called()

    def test_02_invalid_files_logged(self, folder):
        self.write(folder, init_df(price=[100, 1000, 9999]), changes_df(license_id=[1, 2, 99]))
        assert m.main("init.csv", "changed.csv", folder) is False
        logged = " ".join(str(c.args[0]) for c in self.mock_warning.call_args_list)
        assert "initial_licenses.price" in logged
        assert "Validation FAILED" in logged

    def test_03_generated_test_data_is_valid(self):
        folder = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_csv")
        assert m.main("test_initial_licenses.csv", "test_chg_license.csv", folder) is True
