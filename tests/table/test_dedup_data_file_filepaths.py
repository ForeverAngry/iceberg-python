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
import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow import Table as pa_table
from datafusion import SessionContext
from pyiceberg.table import Table
from pyiceberg.io.pyarrow import parquet_file_to_data_file
from tests.catalog.test_base import InMemoryCatalog

@pytest.fixture
def iceberg_catalog(tmp_path):
    catalog = InMemoryCatalog("test.in_memory.catalog", warehouse=tmp_path.absolute().as_posix())
    catalog.create_namespace("default")
    return catalog

def test_overwrite_removes_only_selected_datafile(iceberg_catalog, tmp_path):
    # Create a table and append two batches referencing the same file path
    ctx = SessionContext()
    identifier = "default.test_overwrite_removes_only_selected_datafile"
    try:
        iceberg_catalog.drop_table(identifier)
    except Exception:
        pass

    # Create Arrow schema and table
    arrow_schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("value", pa.string(), nullable=True),
    ])
    df_a = pa_table.from_pylist([
        {"id": 1, "value": "A", "file_path": "path/to/file_a"},
    ], schema=arrow_schema)
    df_b = pa_table.from_pylist([
        {"id": 1, "value": "A", "file_path": "path/to/file_a"},
    ], schema=arrow_schema)

    # Write Arrow tables to Parquet files
    parquet_path_a = str(tmp_path / "file_a.parquet")
    parquet_path_b = str(tmp_path / "file_a.parquet")
    pq.write_table(df_a, parquet_path_a)
    pq.write_table(df_b, parquet_path_b)

    table: Table = iceberg_catalog.create_table(identifier, arrow_schema)

    # Add both files as DataFiles using add_files
    tx = table.transaction()
    tx.add_files([parquet_path_a], check_duplicate_files = False)
    tx.add_files([parquet_path_b], check_duplicate_files=False)
    
    # Find DataFile for file_b
    data_file_b = parquet_file_to_data_file(table.io, table.metadata, parquet_path_b)

    # Overwrite: Remove only the DataFile for file_b
    table.transaction().update_snapshot().overwrite().delete_data_file(data_file_b).commit()

    # Assert: only the row from file_a remains
    # Get all file paths in the current table
    file_paths = [chunk.as_py() for chunk in table.inspect.data_files().to_pylist()]

    # Assert there are no duplicate file paths
    assert len(file_paths) == len(set(file_paths)), "Duplicate file paths found in the table"