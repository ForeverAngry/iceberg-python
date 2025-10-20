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
"""Tests for Transaction data file handling methods."""

from unittest.mock import MagicMock, Mock, patch

import pyarrow as pa
import pytest

from pyiceberg.manifest import DataFile
from pyiceberg.table import Table


class TestIsValidObjectPath:
    """Test the _is_valid_object_path method."""

    def test_valid_s3_path(self, table_v2: Table) -> None:
        """Test that S3 paths are recognized as valid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("s3://bucket/path/to/file.parquet") is True

    def test_valid_gs_path(self, table_v2: Table) -> None:
        """Test that GCS paths are recognized as valid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("gs://bucket/path/to/file.parquet") is True

    def test_valid_file_path(self, table_v2: Table) -> None:
        """Test that file:// paths are recognized as valid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("file:///local/path/to/file.parquet") is True

    def test_valid_hdfs_path(self, table_v2: Table) -> None:
        """Test that HDFS paths are recognized as valid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("hdfs://namenode:9000/path/to/file.parquet") is True

    def test_invalid_relative_path(self, table_v2: Table) -> None:
        """Test that relative paths without scheme are invalid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("relative/path/to/file.parquet") is False

    def test_invalid_absolute_path_no_scheme(self, table_v2: Table) -> None:
        """Test that absolute paths without scheme are invalid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("/absolute/path/to/file.parquet") is False

    def test_invalid_empty_string(self, table_v2: Table) -> None:
        """Test that empty strings are invalid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("") is False

    def test_invalid_whitespace_only(self, table_v2: Table) -> None:
        """Test that whitespace-only strings are invalid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path("   ") is False

    def test_invalid_none(self, table_v2: Table) -> None:
        """Test that None is handled gracefully."""
        txn = table_v2.transaction()
        # None is not a string, so it should return False
        assert txn._is_valid_object_path(None) is False  # type: ignore

    def test_invalid_non_string(self, table_v2: Table) -> None:
        """Test that non-string types are invalid."""
        txn = table_v2.transaction()
        assert txn._is_valid_object_path(123) is False  # type: ignore


class TestDoesFileExist:
    """Test the _does_file_exist method."""

    def test_file_exists(self, table_v2: Table) -> None:
        """Test that existing files return True."""
        txn = table_v2.transaction()

        # Mock the IO to return a file that can be opened
        mock_input = MagicMock()
        mock_input.open.return_value.__enter__ = Mock(return_value=None)
        mock_input.open.return_value.__exit__ = Mock(return_value=None)
        table_v2.io.new_input = Mock(return_value=mock_input)

        assert txn._does_file_exist("s3://bucket/path/to/file.parquet") is True

    def test_file_not_exists(self, table_v2: Table) -> None:
        """Test that non-existing files return False."""
        txn = table_v2.transaction()

        # Mock the IO to raise an exception
        table_v2.io.new_input = Mock(side_effect=FileNotFoundError("File not found"))

        assert txn._does_file_exist("s3://bucket/path/to/nonexistent.parquet") is False

    def test_invalid_path_returns_false(self, table_v2: Table) -> None:
        """Test that invalid paths return False without checking IO."""
        txn = table_v2.transaction()
        assert txn._does_file_exist("relative/path") is False

    def test_io_error_returns_false(self, table_v2: Table) -> None:
        """Test that IO errors return False."""
        txn = table_v2.transaction()

        # Mock the IO to raise a generic exception
        table_v2.io.new_input = Mock(side_effect=Exception("Generic IO error"))

        assert txn._does_file_exist("s3://bucket/path/to/file.parquet") is False


class TestGetDataFilesFromObjects:
    """Test the get_data_files_from_objects method."""

    def test_convert_string_paths_to_data_files(self, table_v2: Table) -> None:
        """Test converting file paths to DataFile objects."""
        txn = table_v2.transaction()

        mock_data_file = Mock(spec=DataFile)
        with patch("pyiceberg.table._parquet_files_to_data_files", return_value=[mock_data_file]):
            result = txn.get_data_files_from_objects(["s3://bucket/path/to/file1.parquet"])

        assert len(result) == 1
        assert result[0] == mock_data_file

    def test_convert_multiple_paths(self, table_v2: Table) -> None:
        """Test converting multiple file paths."""
        txn = table_v2.transaction()

        mock_data_file1 = Mock(spec=DataFile)
        mock_data_file2 = Mock(spec=DataFile)

        def mock_converter(table_metadata, file_paths, io):
            if "file1" in file_paths[0]:
                return [mock_data_file1]
            else:
                return [mock_data_file2]

        with patch("pyiceberg.table._parquet_files_to_data_files", side_effect=mock_converter):
            result = txn.get_data_files_from_objects(
                ["s3://bucket/path/to/file1.parquet", "s3://bucket/path/to/file2.parquet"]
            )

        assert len(result) == 2

    def test_convert_pyarrow_table(self, table_v2: Table) -> None:
        """Test converting PyArrow table to DataFile objects."""
        txn = table_v2.transaction()

        # Create a simple PyArrow table
        df = pa.table({"a": [1, 2, 3], "b": [4, 5, 6]})

        mock_data_file = Mock(spec=DataFile)

        def mock_df_converter(**kwargs):
            return [mock_data_file]

        with patch("pyiceberg.io.pyarrow._dataframe_to_data_files", side_effect=mock_df_converter):
            result = txn.get_data_files_from_objects([df])

        assert len(result) == 1
        assert result[0] == mock_data_file

    def test_mixed_objects_paths_and_tables(self, table_v2: Table) -> None:
        """Test converting mixed list of paths and PyArrow tables."""
        txn = table_v2.transaction()

        df = pa.table({"a": [1, 2, 3]})
        mock_data_file1 = Mock(spec=DataFile)
        mock_data_file2 = Mock(spec=DataFile)

        with patch("pyiceberg.table._parquet_files_to_data_files", return_value=[mock_data_file1]):
            with patch("pyiceberg.io.pyarrow._dataframe_to_data_files", return_value=[mock_data_file2]):
                result = txn.get_data_files_from_objects(["s3://bucket/file.parquet", df])

        assert len(result) == 2

    def test_unsupported_object_type_raises_error(self, table_v2: Table) -> None:
        """Test that unsupported object types raise ValueError."""
        txn = table_v2.transaction()

        with pytest.raises(ValueError, match="Unsupported object type"):
            txn.get_data_files_from_objects([123])  # type: ignore

    def test_unsupported_dict_raises_error(self, table_v2: Table) -> None:
        """Test that dict objects raise ValueError."""
        txn = table_v2.transaction()

        with pytest.raises(ValueError, match="Unsupported object type"):
            txn.get_data_files_from_objects([{"key": "value"}])  # type: ignore

    def test_invalid_path_string_raises_error(self, table_v2: Table) -> None:
        """Test that invalid path strings raise ValueError."""
        txn = table_v2.transaction()

        with pytest.raises(ValueError, match="Unsupported object type"):
            txn.get_data_files_from_objects(["relative/path/without/scheme"])

    def test_empty_list_returns_empty(self, table_v2: Table) -> None:
        """Test that empty list returns empty result."""
        txn = table_v2.transaction()
        result = txn.get_data_files_from_objects([])
        assert result == []


class TestAddDataFiles:
    """Test the add_data_files method."""

    def test_add_data_files_basic(self, table_v2: Table) -> None:
        """Test basic add_data_files functionality."""
        txn = table_v2.transaction()

        mock_data_file = Mock(spec=DataFile)
        mock_snapshot = MagicMock()
        mock_snapshot.__enter__ = Mock(return_value=mock_snapshot)
        mock_snapshot.__exit__ = Mock(return_value=None)

        with patch.object(txn, "update_snapshot") as mock_update:
            mock_fast_append = MagicMock()
            mock_fast_append.fast_append.return_value = mock_snapshot
            mock_update.return_value = mock_fast_append

            txn.add_data_files([mock_data_file])

            # Verify append_data_file was called
            mock_snapshot.append_data_file.assert_called_once_with(mock_data_file)

    def test_add_data_files_with_snapshot_properties(self, table_v2: Table) -> None:
        """Test add_data_files with custom snapshot properties."""
        txn = table_v2.transaction()

        mock_data_file = Mock(spec=DataFile)
        mock_snapshot = MagicMock()
        mock_snapshot.__enter__ = Mock(return_value=mock_snapshot)
        mock_snapshot.__exit__ = Mock(return_value=None)

        snapshot_props = {"custom_prop": "custom_value"}

        with patch.object(txn, "update_snapshot") as mock_update:
            mock_fast_append = MagicMock()
            mock_fast_append.fast_append.return_value = mock_snapshot
            mock_update.return_value = mock_fast_append

            txn.add_data_files([mock_data_file], snapshot_properties=snapshot_props)

            mock_update.assert_called_once_with(snapshot_properties=snapshot_props)

    def test_add_multiple_data_files(self, table_v2: Table) -> None:
        """Test adding multiple data files."""
        txn = table_v2.transaction()

        mock_data_files = [Mock(spec=DataFile) for _ in range(3)]
        mock_snapshot = MagicMock()
        mock_snapshot.__enter__ = Mock(return_value=mock_snapshot)
        mock_snapshot.__exit__ = Mock(return_value=None)

        with patch.object(txn, "update_snapshot") as mock_update:
            mock_fast_append = MagicMock()
            mock_fast_append.fast_append.return_value = mock_snapshot
            mock_update.return_value = mock_fast_append

            txn.add_data_files(mock_data_files)

            # Verify append_data_file was called for each file
            assert mock_snapshot.append_data_file.call_count == 3

    def test_add_data_files_empty_list(self, table_v2: Table) -> None:
        """Test that empty list returns early without operations."""
        txn = table_v2.transaction()

        with patch.object(txn, "update_snapshot") as mock_update:
            txn.add_data_files([])

            # Should not call update_snapshot
            mock_update.assert_not_called()

    def test_add_data_files_calls_append_data_file(self, table_v2: Table) -> None:
        """Test that add_data_files properly calls append_data_file for each file."""
        txn = table_v2.transaction()

        mock_data_files = [Mock(spec=DataFile), Mock(spec=DataFile)]
        mock_snapshot = MagicMock()
        mock_snapshot.__enter__ = Mock(return_value=mock_snapshot)
        mock_snapshot.__exit__ = Mock(return_value=None)

        with patch.object(txn, "update_snapshot") as mock_update:
            mock_fast_append = MagicMock()
            mock_fast_append.fast_append.return_value = mock_snapshot
            mock_update.return_value = mock_fast_append

            txn.add_data_files(mock_data_files)

            # Verify append_data_file was called for each file
            assert mock_snapshot.append_data_file.call_count == len(mock_data_files)


class TestIntegrationGetDataFilesAndAdd:
    """Integration tests for get_data_files_from_objects and add_data_files."""

    def test_full_workflow_with_paths(self, table_v2: Table) -> None:
        """Test full workflow: get data files from paths and add them."""
        txn = table_v2.transaction()

        mock_data_file = Mock(spec=DataFile)
        mock_snapshot = MagicMock()
        mock_snapshot.__enter__ = Mock(return_value=mock_snapshot)
        mock_snapshot.__exit__ = Mock(return_value=None)

        with patch("pyiceberg.table._parquet_files_to_data_files", return_value=[mock_data_file]):
            with patch.object(txn, "update_snapshot") as mock_update:
                mock_fast_append = MagicMock()
                mock_fast_append.fast_append.return_value = mock_snapshot
                mock_update.return_value = mock_fast_append

                # Get data files from paths
                data_files = txn.get_data_files_from_objects(["s3://bucket/file.parquet"])

                # Add them to the transaction
                txn.add_data_files(data_files)

                # Verify the workflow
                assert len(data_files) == 1
                mock_snapshot.append_data_file.assert_called_once()

    def test_full_workflow_with_pyarrow_table(self, table_v2: Table) -> None:
        """Test full workflow: get data files from PyArrow table and add them."""
        txn = table_v2.transaction()

        df = pa.table({"a": [1, 2, 3]})
        mock_data_file = Mock(spec=DataFile)
        mock_snapshot = MagicMock()
        mock_snapshot.__enter__ = Mock(return_value=mock_snapshot)
        mock_snapshot.__exit__ = Mock(return_value=None)

        with patch("pyiceberg.io.pyarrow._dataframe_to_data_files", return_value=[mock_data_file]):
            with patch.object(txn, "update_snapshot") as mock_update:
                mock_fast_append = MagicMock()
                mock_fast_append.fast_append.return_value = mock_snapshot
                mock_update.return_value = mock_fast_append

                # Get data files from PyArrow table
                data_files = txn.get_data_files_from_objects([df])

                # Add them to the transaction
                txn.add_data_files(data_files)

                # Verify the workflow
                assert len(data_files) == 1
                mock_snapshot.append_data_file.assert_called_once()
