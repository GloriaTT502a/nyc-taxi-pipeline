# tests/test_transformations.py

from bronze.transformations import (
    normalize_dataframe
)

from tests.helpers import (
    create_input_df
)


class TestNormalizeDataFrame:

    """
    测试 normalize_dataframe
    """

    def test_should_rename_vendor_id(
        self,
        spark
    ):

        # =========================
        # Arrange
        # =========================

        df = create_input_df(spark)

        # =========================
        # Act
        # =========================

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        # =========================
        # Assert
        # =========================

        assert result.vendor_id == "1"

    def test_should_keep_passenger_count(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        assert result.passenger_count == 2

    def test_should_keep_total_amount(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        assert result.total_amount == 30.5

    def test_should_extract_yyyy(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        assert result.YYYY == 2010

    def test_should_extract_yyyymm(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        assert result.YYYYMM == 201001

    def test_should_add_run_id(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        assert result._run_id == "test-run-id"

    def test_should_add_input_file(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        result = result_df.collect()[0]

        assert (
            result._input_file
            ==
            "yellow_tripdata_2010-01.parquet"
        )

    def test_should_have_expected_columns(
        self,
        spark
    ):

        df = create_input_df(spark)

        result_df = normalize_dataframe(
            df,
            run_id="test-run-id",
            lineage_col="_input_file",
            run_id_col="_run_id"
        )

        expected_columns = {

            "vendor_id",
            "passenger_count",
            "total_amount",
            "YYYY",
            "YYYYMM",
            "_run_id",
            "_input_file"
        }

        actual_columns = set(result_df.columns)

        assert (
            expected_columns
            .issubset(actual_columns)
        )