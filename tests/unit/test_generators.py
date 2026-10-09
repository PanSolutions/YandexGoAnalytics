from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from src.core.utils.data_generation.driver import DriverGenerator
from src.core.utils.data_generation.trip import TripGenerator


class TestDataGenerators:
    """Test suite verifying synthetic data generators output and constraints."""

    @patch("src.core.utils.data_generation.base.get_catalog", return_value="unit_cat")
    def test_driver_pool_determinism(self, _: object) -> None:
        """Ensure driver ID pools generate stable deterministic output."""
        pool_1 = DriverGenerator.get_driver_id_pool(10)
        pool_2 = DriverGenerator.get_driver_id_pool(10)
        assert len(pool_1) == 10
        assert pool_1 == pool_2

    @patch("src.core.utils.data_generation.base.get_catalog", return_value="unit_cat")
    def test_driver_generator_dataframe_build(
        self, _: object, spark_mock_session: SparkSession
    ) -> None:
        """Validate generated Driver DataFrame schema and row count."""
        generator = DriverGenerator(pool_size=10, anomaly_rate=0.0)

        # Мокируем создание датафрейма генератором
        mock_df = MagicMock()
        mock_df.count.return_value = 5
        mock_df.columns = ["id", "name", "rating", "experience", "phone"]
        spark_mock_session.createDataFrame.return_value = mock_df

        df = generator.generate_batch_df(spark_mock_session, row_count=5, with_phone=True)

        assert df.count() == 5
        assert "phone" in df.columns
        assert "rating" in df.columns

    @patch("src.core.utils.data_generation.base.get_catalog", return_value="unit_cat")
    def test_generator_path_interpolation(self, _: object) -> None:
        """Validate Unity Catalog volume path resolution."""
        generator = TripGenerator(subfolder="trips_data", volume_name="landing")
        expected_path = "/Volumes/unit_cat/raw_files/landing/trips_data"
        assert generator.output_path == expected_path
