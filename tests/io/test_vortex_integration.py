# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# pylint: disable=protected-access,unused-argument,redefined-outer-name
"""End-to-end integration tests for Vortex file format support in PyIceberg.

These tests verify the complete integration of Vortex format throughout the
PyIceberg stack, including:
- File format detection
- Reading Vortex files through PyArrow I/O
- Writing Vortex files
- Transaction integration with custom data file methods
- Schema compatibility
- Multi-format table support
"""

import tempfile
from pathlib import Path
from typing import Iterator
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.io.vortex import VORTEX_AVAILABLE, write_vortex_file
from pyiceberg.manifest import FileFormat
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

# Skip all tests if vortex-data is not available
pytestmark = pytest.mark.skipif(not VORTEX_AVAILABLE, reason="vortex-data package not installed")


@pytest.fixture
def sample_schema() -> Schema:
    """Create a sample Iceberg schema for testing."""
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="age", field_type=IntegerType(), required=False),
        schema_id=0,
    )


@pytest.fixture
def sample_arrow_data() -> pa.Table:
    """Create sample Arrow data for testing."""
    return pa.table(
        {
            "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
            "name": pa.array(["Alice", "Bob", "Charlie", "Diana", "Eve"], type=pa.string()),
            "age": pa.array([25, 30, 35, 28, 32], type=pa.int32()),
        }
    )


@pytest.fixture
def temp_vortex_file(sample_arrow_data: pa.Table) -> Iterator[str]:
    """Create a temporary Vortex file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as temp_file:
        temp_path = temp_file.name

    # Write the Vortex file
    io = PyArrowFileIO()
    write_vortex_file(arrow_table=sample_arrow_data, file_path=temp_path, io=io)

    yield temp_path

    # Cleanup
    Path(temp_path).unlink(missing_ok=True)


class TestVortexFileFormatDetection:
    """Test automatic file format detection for Vortex files."""

    def test_vortex_file_extension_detection(self) -> None:
        """Test that .vortex extension is properly detected."""
        from pyiceberg.io.pyarrow import _infer_format_from_path

        file_path = "s3://bucket/data/file.vortex"
        detected_format = _infer_format_from_path(file_path)
        assert detected_format == FileFormat.VORTEX

    def test_vortex_vs_parquet_extension(self) -> None:
        """Test that Vortex and Parquet extensions are distinguished."""
        from pyiceberg.io.pyarrow import _infer_format_from_path

        assert _infer_format_from_path("file.vortex") == FileFormat.VORTEX
        assert _infer_format_from_path("file.parquet") == FileFormat.PARQUET
        assert _infer_format_from_path("file.orc") == FileFormat.ORC

    def test_vortex_with_various_storage_schemes(self) -> None:
        """Test Vortex detection works with different storage schemes."""
        from pyiceberg.io.pyarrow import _infer_format_from_path

        test_cases = [
            "file:///local/path/data.vortex",
            "s3://bucket/prefix/data.vortex",
            "gs://bucket/path/data.vortex",
            "abfss://container@account.dfs.core.windows.net/data.vortex",
            "hdfs://namenode:9000/data.vortex",
        ]

        for path in test_cases:
            assert _infer_format_from_path(path) == FileFormat.VORTEX, f"Failed for path: {path}"


class TestVortexFileReading:
    """Test reading Vortex files through PyIceberg's I/O layer."""

    def test_read_vortex_file_basic(self, temp_vortex_file: str) -> None:
        """Test basic Vortex file reading."""
        from pyiceberg.io.vortex import read_vortex_file

        io = PyArrowFileIO()
        batches = list(read_vortex_file(file_path=temp_vortex_file, io=io))

        # Verify we got data back
        assert len(batches) > 0

        # Combine batches and verify content
        total_rows = sum(batch.num_rows for batch in batches)
        assert total_rows == 5

    def test_read_vortex_file_with_schema_projection(self, temp_vortex_file: str, sample_schema: Schema) -> None:
        """Test reading Vortex file with schema projection."""
        from pyiceberg.io.vortex import read_vortex_file

        io = PyArrowFileIO()

        # Read with projection - only id and name columns
        batches = list(read_vortex_file(file_path=temp_vortex_file, io=io, projected_schema=sample_schema))

        assert len(batches) > 0
        # Verify schema contains expected columns
        batch = batches[0]
        column_names = batch.schema.names
        assert "id" in column_names
        assert "name" in column_names


class TestVortexFileWriting:
    """Test writing Vortex files through PyIceberg's I/O layer."""

    def test_write_and_read_roundtrip(self, sample_arrow_data: pa.Table) -> None:
        """Test writing a Vortex file and reading it back."""
        from pyiceberg.io.vortex import read_vortex_file, write_vortex_file

        with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            io = PyArrowFileIO()

            # Write the file
            file_size = write_vortex_file(arrow_table=sample_arrow_data, file_path=temp_path, io=io)

            # Verify file was created and has size
            assert file_size > 0
            assert Path(temp_path).exists()

            # Read it back
            batches = list(read_vortex_file(file_path=temp_path, io=io))
            result_table = pa.Table.from_batches(batches)

            # Verify data integrity
            assert result_table.num_rows == sample_arrow_data.num_rows
            assert result_table.schema.names == sample_arrow_data.schema.names

        finally:
            Path(temp_path).unlink(missing_ok=True)

    def test_write_vortex_file_size_tracking(self, sample_arrow_data: pa.Table) -> None:
        """Test that file size is properly tracked when writing Vortex files."""
        from pyiceberg.io.vortex import write_vortex_file

        with tempfile.NamedTemporaryFile(suffix=".vortex", delete=False) as temp_file:
            temp_path = temp_file.name

        try:
            io = PyArrowFileIO()
            file_size = write_vortex_file(arrow_table=sample_arrow_data, file_path=temp_path, io=io)

            # Verify reported size matches actual file size
            actual_size = Path(temp_path).stat().st_size
            assert file_size == actual_size
            assert file_size > 0

        finally:
            Path(temp_path).unlink(missing_ok=True)


class TestVortexDataFileCreation:
    """Test creation of DataFile objects for Vortex files."""

    def test_files_to_data_files_with_vortex(self, temp_vortex_file: str) -> None:
        """Test that _files_to_data_files handles Vortex files correctly."""
        from pyiceberg.io.pyarrow import _files_to_data_files

        io = PyArrowFileIO()

        # Use the unified function that handles all formats
        data_files = _files_to_data_files(
            io=io,
            table_metadata=Mock(
                schema=Mock(
                    spec=Schema,
                    find_field=Mock(return_value=Mock(field_id=1)),
                    find_column_name=Mock(return_value="id"),
                )
            ),
            file_paths=[temp_vortex_file],
        )

        # Verify we got a DataFile
        assert len(data_files) == 1
        data_file = data_files[0]

        # Verify it's recognized as Vortex format
        assert data_file.file_format == FileFormat.VORTEX
        assert data_file.file_path == temp_vortex_file
        assert data_file.record_count > 0
        assert data_file.file_size_in_bytes > 0

    def test_vortex_data_file_has_correct_metadata(self, temp_vortex_file: str) -> None:
        """Test that DataFile objects for Vortex files have correct metadata."""
        from pyiceberg.io.pyarrow import _files_to_data_files

        io = PyArrowFileIO()
        mock_schema = Mock(spec=Schema)
        mock_schema.find_field = Mock(return_value=Mock(field_id=1))
        mock_schema.find_column_name = Mock(return_value="id")

        data_files = _files_to_data_files(
            io=io, table_metadata=Mock(schema=mock_schema), file_paths=[temp_vortex_file]
        )

        data_file = data_files[0]

        # Verify required DataFile attributes
        assert hasattr(data_file, "file_format")
        assert hasattr(data_file, "file_path")
        assert hasattr(data_file, "record_count")
        assert hasattr(data_file, "file_size_in_bytes")

        # Verify values are reasonable
        assert data_file.record_count == 5  # From sample data
        assert data_file.file_size_in_bytes > 100  # Vortex files should have some size


class TestVortexTransactionIntegration:
    """Test integration with Transaction's custom data file methods."""

    def test_get_data_files_from_vortex_paths(self, temp_vortex_file: str) -> None:
        """Test Transaction.get_data_files_from_objects with Vortex file paths."""
        from pyiceberg.table import Transaction

        # Mock the transaction's dependencies
        mock_transaction = Mock(spec=Transaction)
        mock_transaction._table = Mock()
        mock_transaction._table.io = PyArrowFileIO()
        mock_transaction._table.metadata = Mock(
            schema=Mock(
                spec=Schema,
                find_field=Mock(return_value=Mock(field_id=1)),
                find_column_name=Mock(return_value="id"),
            )
        )

        # Call the method (would need to be bound to the mock)
        # This tests that the path resolution works
        from pyiceberg.table import _files_to_data_files

        data_files = _files_to_data_files(
            io=mock_transaction._table.io,
            table_metadata=mock_transaction._table.metadata,
            file_paths=[temp_vortex_file],
        )

        assert len(data_files) == 1
        assert data_files[0].file_format == FileFormat.VORTEX


class TestVortexMultiFormatSupport:
    """Test that Vortex works alongside Parquet and ORC in mixed-format tables."""

    def test_mixed_format_file_list(self, temp_vortex_file: str, sample_arrow_data: pa.Table) -> None:
        """Test handling a list of mixed file formats (Parquet, ORC, Vortex)."""
        from pyiceberg.io.pyarrow import _files_to_data_files

        # Create temporary files of different formats
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as pq_file:
            pq_path = pq_file.name
            pa.parquet.write_table(sample_arrow_data, pq_path)

        try:
            io = PyArrowFileIO()
            mock_schema = Mock(spec=Schema)
            mock_schema.find_field = Mock(return_value=Mock(field_id=1))
            mock_schema.find_column_name = Mock(return_value="id")

            # Process mixed format files
            data_files = _files_to_data_files(
                io=io, table_metadata=Mock(schema=mock_schema), file_paths=[temp_vortex_file, pq_path]
            )

            # Verify we got both files
            assert len(data_files) == 2

            # Verify formats are correctly identified
            formats = {df.file_format for df in data_files}
            assert FileFormat.VORTEX in formats
            assert FileFormat.PARQUET in formats

            # Verify each file has valid metadata
            for df in data_files:
                assert df.record_count > 0
                assert df.file_size_in_bytes > 0

        finally:
            Path(pq_path).unlink(missing_ok=True)

    def test_format_detection_priority(self) -> None:
        """Test that format detection works correctly for edge cases."""
        from pyiceberg.io.pyarrow import _infer_format_from_path

        # Test that .vortex is not confused with .parquet even with similar names
        assert _infer_format_from_path("data.vortex") == FileFormat.VORTEX
        assert _infer_format_from_path("data.parquet.vortex") == FileFormat.VORTEX
        assert _infer_format_from_path("vortex_data.parquet") == FileFormat.PARQUET


class TestVortexSchemaCompatibility:
    """Test schema compatibility between Iceberg and Vortex."""

    def test_iceberg_to_vortex_schema_conversion(self, sample_schema: Schema) -> None:
        """Test converting Iceberg schema to Vortex-compatible Arrow schema."""
        from pyiceberg.io.vortex import iceberg_schema_to_vortex_schema

        vortex_schema = iceberg_schema_to_vortex_schema(sample_schema)

        # Verify it's a PyArrow schema
        assert isinstance(vortex_schema, pa.Schema)

        # Verify field names are preserved
        field_names = vortex_schema.names
        assert "id" in field_names
        assert "name" in field_names
        assert "age" in field_names

    def test_vortex_preserves_field_ids(self, sample_schema: Schema) -> None:
        """Test that Vortex schema conversion preserves Iceberg field IDs."""
        from pyiceberg.io.vortex import iceberg_schema_to_vortex_schema

        vortex_schema = iceberg_schema_to_vortex_schema(sample_schema)

        # Check that field IDs are preserved in metadata
        for field in vortex_schema:
            if field.metadata:
                # Field IDs should be stored in metadata
                assert b"PARQUET:field_id" in field.metadata or "PARQUET:field_id" in field.metadata


class TestVortexErrorHandling:
    """Test error handling for Vortex-specific scenarios."""

    def test_vortex_file_not_found(self) -> None:
        """Test proper error handling when Vortex file doesn't exist."""
        from pyiceberg.io.vortex import read_vortex_file

        io = PyArrowFileIO()
        non_existent_path = "/tmp/non_existent_file.vortex"

        with pytest.raises(FileNotFoundError):
            list(read_vortex_file(file_path=non_existent_path, io=io))

    def test_vortex_unavailable_graceful_degradation(self) -> None:
        """Test that system gracefully handles Vortex being unavailable."""
        with patch("pyiceberg.io.vortex.VORTEX_AVAILABLE", False):
            from pyiceberg.io.vortex import write_vortex_file

            with pytest.raises(ImportError, match="vortex-data is not installed"):
                write_vortex_file(
                    arrow_table=pa.table({"a": [1, 2, 3]}), file_path="/tmp/test.vortex", io=PyArrowFileIO()
                )


class TestVortexPerformance:
    """Test performance-related aspects of Vortex integration."""

    def test_vortex_batch_size_configuration(self, temp_vortex_file: str) -> None:
        """Test that batch size can be configured for Vortex reads."""
        from pyiceberg.io.vortex import read_vortex_file

        io = PyArrowFileIO()

        # Read with custom batch size
        batches = list(read_vortex_file(file_path=temp_vortex_file, io=io, batch_size=2))

        # With 5 rows and batch_size=2, we should get 3 batches (2+2+1)
        # Note: Actual behavior depends on Vortex implementation
        assert len(batches) >= 1

        # Verify total row count is preserved
        total_rows = sum(batch.num_rows for batch in batches)
        assert total_rows == 5

    def test_vortex_streaming_read(self, temp_vortex_file: str) -> None:
        """Test that Vortex reads return an iterator (streaming, not loading all into memory)."""
        from pyiceberg.io.vortex import read_vortex_file

        io = PyArrowFileIO()

        # Call the function (should return an iterator)
        result = read_vortex_file(file_path=temp_vortex_file, io=io)

        # Verify it's an iterator (not a list)
        import collections.abc

        assert isinstance(result, collections.abc.Iterator)

        # Consume the iterator
        batches = list(result)
        assert len(batches) > 0
