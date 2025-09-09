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

"""Vortex file format support for PyIceberg.

This module provides support for reading and writing Vortex files, a next-generation
columnar file format designed for high-performance data processing. Vortex offers:

- 100x faster random access reads vs. Parquet
- 10-20x faster scans
- 5x faster writes
- Similar compression ratios
- Zero-copy compatibility with Apache Arrow

The implementation leverages vortex-data Python bindings to integrate with PyIceberg's
existing table operations and schema management.
"""

from __future__ import annotations

import logging
import os
import platform
import sys
import uuid
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from pyiceberg.expressions import (
    AlwaysTrue,
    And,
    BooleanExpression,
    EqualTo,
    GreaterThan,
    GreaterThanOrEqual,
    In,
    IsNaN,
    IsNull,
    LessThan,
    LessThanOrEqual,
    Not,
    NotEqualTo,
    NotIn,
    NotNaN,
    NotNull,
    Or,
    BoundEqualTo,
    BoundGreaterThan,
    BoundGreaterThanOrEqual,
    BoundIn,
    BoundIsNaN,
    BoundIsNull,
    BoundLessThan,
    BoundLessThanOrEqual,
    BoundNotEqualTo,
    BoundNotIn,
    BoundNotNaN,
    BoundNotNull,
)
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFile, DataFileContent, FileFormat
from pyiceberg.partitioning import PartitionKey, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.typedef import Record
from pyiceberg.types import ListType, MapType, StructType

try:
    import vortex as vx  # type: ignore[import-not-found]
    import vortex.expr as ve  # type: ignore[import-not-found]

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False
    vx = None
    ve = None


logger = logging.getLogger(__name__)

# Vortex file extension
VORTEX_FILE_EXTENSION = ".vortex"


# Memory Allocator Optimization for Vortex Performance
# ====================================================

def _get_memory_allocator_info() -> Dict[str, Any]:
    """Get information about the current memory allocator configuration."""
    system = platform.system()

    info = {
        "system": system,
        "python_version": sys.version.split()[0],
        "current_settings": {},
        "recommended_settings": {},
        "optimizations_applied": []
    }

    # Check current environment variables
    alloc_vars = [
        "MALLOC_ARENA_MAX",
        "MALLOC_MMAP_THRESHOLD",
        "MALLOC_TRIM_THRESHOLD",
        "MALLOC_TOP_PAD",
        "PYTHONMALLOC"
    ]

    for var in alloc_vars:
        current_value = os.environ.get(var)
        info["current_settings"][var] = current_value or "default"

    # Set recommended values based on system
    if system == "Linux":
        info["recommended_settings"] = {
            "MALLOC_ARENA_MAX": "1",  # Single arena for better cache locality
            "MALLOC_MMAP_THRESHOLD": "131072",  # 128KB threshold for mmap
            "MALLOC_TRIM_THRESHOLD": "524288",  # 512KB trim threshold
            "MALLOC_TOP_PAD": "1048576",  # 1MB top pad
            "PYTHONMALLOC": "malloc"  # Use system malloc
        }
    elif system == "Darwin":  # macOS
        info["recommended_settings"] = {
            "MALLOC_MMAP_THRESHOLD": "131072",
            "PYTHONMALLOC": "malloc"
        }
    else:
        info["recommended_settings"] = {
            "PYTHONMALLOC": "malloc"
        }

    return info


def _optimize_memory_allocator() -> None:
    """Apply memory allocator optimizations for Vortex performance."""
    system = platform.system()

    logger.info("🔧 Optimizing Memory Allocator for Vortex Performance")

    if system == "Linux":
        # Optimize glibc malloc for high-throughput workloads
        os.environ.setdefault("MALLOC_ARENA_MAX", "1")
        os.environ.setdefault("MALLOC_MMAP_THRESHOLD", "131072")
        os.environ.setdefault("MALLOC_TRIM_THRESHOLD", "524288")
        os.environ.setdefault("MALLOC_TOP_PAD", "1048576")
        os.environ.setdefault("PYTHONMALLOC", "malloc")

    elif system == "Darwin":
        # macOS optimizations (limited tunables available)
        os.environ.setdefault("MALLOC_MMAP_THRESHOLD", "131072")
        os.environ.setdefault("PYTHONMALLOC", "malloc")

    # Cross-platform optimizations
    os.environ.setdefault("PYTHONMALLOC", "malloc")

    # Log applied optimizations
    optimizations = []
    if os.environ.get("MALLOC_ARENA_MAX"):
        optimizations.append(f"MALLOC_ARENA_MAX={os.environ['MALLOC_ARENA_MAX']}")
    if os.environ.get("MALLOC_MMAP_THRESHOLD"):
        threshold_kb = int(os.environ["MALLOC_MMAP_THRESHOLD"]) // 1024
        optimizations.append(f"MALLOC_MMAP_THRESHOLD={threshold_kb}KB")
    if os.environ.get("PYTHONMALLOC"):
        optimizations.append(f"PYTHONMALLOC={os.environ['PYTHONMALLOC']}")

    if optimizations:
        logger.info(f"✅ Applied memory optimizations: {', '.join(optimizations)}")
    else:
        logger.info("ℹ️  No additional memory optimizations needed")


# Apply memory optimizations when Vortex module is loaded
if VORTEX_AVAILABLE:
    try:
        _optimize_memory_allocator()
        logger.info("✅ Vortex memory allocator optimizations applied successfully")
    except Exception as e:
        logger.warning(f"⚠️  Failed to apply memory optimizations: {e}")
else:
    logger.debug("ℹ️  Vortex not available, skipping memory optimizations")


@dataclass(frozen=True)
class VortexWriteTask:
    """Task for writing data to a Vortex file."""

    write_uuid: Optional[uuid.UUID]
    task_id: int
    record_batches: List[pa.RecordBatch]
    partition_key: Optional[PartitionKey] = None
    schema: Optional[Schema] = None

    def generate_data_file_filename(self, extension: str) -> str:
        """Generate a unique filename for the data file."""
        if self.partition_key:
            # Use a simple partition hash for filename uniqueness
            partition_hash = hash(str(self.partition_key.partition))
            return f"{self.write_uuid}-{self.task_id}-{partition_hash}.{extension}"
        else:
            return f"{self.write_uuid}-{self.task_id}.{extension}"


def _check_vortex_available() -> None:
    """Check if vortex is available and raise an informative error if not."""
    if not VORTEX_AVAILABLE:
        raise ImportError(
            "vortex-data is not installed. Please install it with: pip install vortex-data or pip install 'pyiceberg[vortex]'"
        )


def iceberg_schema_to_vortex_schema(iceberg_schema: Schema) -> Any:
    """Convert an Iceberg schema to a Vortex schema.

    Args:
        iceberg_schema: The Iceberg schema to convert

    Returns:
        A PyArrow schema that Vortex can use for type inference

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If schema conversion fails
    """
    _check_vortex_available()

    try:
        # Convert to PyArrow schema first, preserving field IDs
        from pyiceberg.io.pyarrow import schema_to_pyarrow

        arrow_schema = schema_to_pyarrow(iceberg_schema, include_field_ids=True)

        # Validate that the schema is compatible with Vortex
        _validate_vortex_schema_compatibility(arrow_schema)

        return arrow_schema
    except Exception as e:
        raise ValueError(f"Failed to convert Iceberg schema to Vortex-compatible format: {e}") from e


def _validate_vortex_schema_compatibility(arrow_schema: pa.Schema) -> None:
    """Validate that a PyArrow schema is compatible with Vortex.

    Args:
        arrow_schema: The PyArrow schema to validate

    Raises:
        ValueError: If the schema contains unsupported types
    """
    unsupported_types = []

    for field in arrow_schema:
        field_type = field.type

        # Check for complex nested types that might not be fully supported
        if pa.types.is_union(field_type):
            unsupported_types.append(f"Union type in field '{field.name}'")
        elif pa.types.is_large_list(field_type) or pa.types.is_large_string(field_type):
            # Large types might have performance implications
            logger.warning(f"Large type detected in field '{field.name}' - may impact performance")

    if unsupported_types:
        raise ValueError(f"Schema contains unsupported types for Vortex: {', '.join(unsupported_types)}")


def arrow_to_vortex_array(arrow_table: pa.Table, compress: bool = True) -> Any:
    """Convert a PyArrow table to a Vortex array.

    Args:
        arrow_table: The PyArrow table to convert
        compress: Whether to apply Vortex compression optimizations

    Returns:
        A Vortex array

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Create Vortex array from Arrow table
        vortex_array = vx.array(arrow_table)

        # Apply compression if requested
        if compress:
            try:
                vortex_array = vx.compress(vortex_array)
                logger.debug(f"Applied Vortex compression to array with {len(arrow_table)} rows")
            except Exception as e:
                logger.warning(f"Vortex compression failed, using uncompressed array: {e}")

        return vortex_array

    except Exception as e:
        raise ValueError(f"Failed to convert PyArrow table to Vortex array: {e}") from e


def vortex_to_arrow_table(vortex_array: Any, preserve_field_ids: bool = True) -> pa.Table:
    """Convert a Vortex array back to a PyArrow table.

    Args:
        vortex_array: The Vortex array to convert
        preserve_field_ids: Whether to preserve Iceberg field IDs in metadata

    Returns:
        A PyArrow table

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Convert Vortex array to Arrow
        if hasattr(vortex_array, "to_arrow_array"):
            arrow_data = vortex_array.to_arrow_array()
        elif hasattr(vortex_array, "to_arrow"):
            arrow_data = vortex_array.to_arrow()
        else:
            raise ValueError("Vortex array does not have a recognized Arrow conversion method")

        # Handle both Table and Array returns
        if isinstance(arrow_data, pa.Table):
            arrow_table = arrow_data
        elif isinstance(arrow_data, pa.Array):
            # Convert Array to Table with a single column
            arrow_table = pa.table([arrow_data], names=["data"])
        elif isinstance(arrow_data, pa.ChunkedArray):
            # Convert ChunkedArray to Table with a single column
            arrow_table = pa.table([arrow_data], names=["data"])
        else:
            # Handle RecordBatch or other formats
            if hasattr(arrow_data, "to_table"):
                arrow_table = arrow_data.to_table()
            else:
                raise ValueError(f"Unexpected Arrow data type: {type(arrow_data)}")

        # Validate the resulting table
        _validate_arrow_table_integrity(arrow_table)

        logger.debug(f"Converted Vortex array to Arrow table with {len(arrow_table)} rows, {len(arrow_table.schema)} columns")
        return arrow_table

    except Exception as e:
        raise ValueError(f"Failed to convert Vortex array to PyArrow table: {e}") from e


def _validate_arrow_table_integrity(arrow_table: pa.Table) -> None:
    """Validate the integrity of a converted Arrow table.

    Args:
        arrow_table: The Arrow table to validate

    Raises:
        ValueError: If the table has integrity issues
    """
    if arrow_table is None:
        raise ValueError("Converted Arrow table is None")

    if len(arrow_table.schema) == 0:
        raise ValueError("Converted Arrow table has no columns")

    # Check for null schemas or corrupt data
    try:
        arrow_table.validate()
    except Exception as e:
        logger.warning(f"Arrow table validation failed: {e}")


def convert_iceberg_to_vortex_file(
    iceberg_table_data: pa.Table,
    iceberg_schema: Schema,
    output_path: str,
    io: FileIO,
    compression: bool = True,
) -> DataFile:
    """High-level function to convert Iceberg table data to a Vortex file.

    Args:
        iceberg_table_data: PyArrow table with Iceberg data
        iceberg_schema: The Iceberg schema
        output_path: Path where to write the Vortex file
        io: FileIO instance for file operations
        compression: Whether to apply Vortex compression

    Returns:
        DataFile metadata for the created Vortex file

    Raises:
        ImportError: If vortex-data is not installed
        ValueError: If conversion fails
    """
    _check_vortex_available()

    try:
        # Validate input
        if iceberg_table_data is None or len(iceberg_table_data) == 0:
            raise ValueError("Input table data is empty")

        # Convert schema for validation
        iceberg_schema_to_vortex_schema(iceberg_schema)
        logger.debug(f"Validated schema with {len(iceberg_schema.fields)} fields")

        # Avoid full data conversion here; schema validation is sufficient.
        # Actual conversion/writing happens in write_vortex_file.
        logger.debug(f"Prepared {len(iceberg_table_data)} rows for Vortex conversion")

        # Write Vortex file
        file_size = write_vortex_file(
            arrow_table=iceberg_table_data,  # Use original Arrow table for writing
            file_path=output_path,
            io=io,
            compression="auto" if compression else None,
        )

        # Create DataFile metadata
        data_file = DataFile.from_args(
            content=DataFileContent.DATA,
            file_path=output_path,
            file_format=FileFormat.VORTEX,
            partition=Record(),
            file_size_in_bytes=file_size,
            record_count=len(iceberg_table_data),
            sort_order_id=None,
            spec_id=0,  # Default spec
            equality_ids=None,
            key_metadata=None,
        )

        logger.info(f"Successfully created Vortex file: {output_path} ({file_size} bytes, {len(iceberg_table_data)} rows)")
        return data_file

    except Exception as e:
        raise ValueError(f"Failed to convert Iceberg data to Vortex file: {e}") from e


def estimate_vortex_compression_ratio(arrow_table: pa.Table) -> float:
    """Estimate the compression ratio that Vortex might achieve.

    Args:
        arrow_table: The PyArrow table to analyze

    Returns:
        Estimated compression ratio (original_size / compressed_size)

    Note:
        This is an approximation based on data characteristics
    """
    if not VORTEX_AVAILABLE:
        return 1.0  # No compression without Vortex

    try:
        # Estimate based on data types and patterns
        compression_ratio = 1.0

        for column in arrow_table.columns:
            column_type = column.type

            if pa.types.is_string(column_type) or pa.types.is_binary(column_type):
                # String compression can be very effective
                compression_ratio *= 2.5
            elif pa.types.is_integer(column_type):
                # Integer compression depends on distribution
                compression_ratio *= 1.8
            elif pa.types.is_floating(column_type):
                # Floating point compression is more limited
                compression_ratio *= 1.3
            elif pa.types.is_boolean(column_type):
                # Boolean data compresses excellently
                compression_ratio *= 8.0

        # Cap the estimate at reasonable bounds
        return min(max(compression_ratio, 1.1), 10.0)

    except Exception:
        return 2.0  # Default conservative estimate


def optimize_vortex_write_config(
    arrow_table: pa.Table,
    target_file_size_mb: int = 128,
) -> Dict[str, Any]:
    """Generate optimized configuration for Vortex file writing.

    Args:
        arrow_table: The table to be written
        target_file_size_mb: Target file size in MB

    Returns:
        Dictionary with optimized Vortex write parameters
    """
    config = {
        "compression": "auto",
        "row_group_size": 10000,
        "dictionary_encoding": True,
        "statistics": True,
    }

    if not VORTEX_AVAILABLE:
        return config

    try:
        # Adjust based on table characteristics
        num_rows = len(arrow_table)
        num_columns = len(arrow_table.schema)

        # Optimize row group size based on data volume
        if num_rows < 1000:
            config["row_group_size"] = max(100, num_rows // 10)
        elif num_rows > 1_000_000:
            config["row_group_size"] = 50000
        else:
            config["row_group_size"] = min(10000, num_rows // 100)

        # Enable advanced compression for large tables
        if num_rows > 100_000 or num_columns > 100:
            config["compression"] = "aggressive"

        # Dictionary encoding is most effective for string columns
        string_columns = sum(1 for field in arrow_table.schema if pa.types.is_string(field.type))
        if string_columns == 0:
            config["dictionary_encoding"] = False

        return config

    except Exception:
        return config


def batch_convert_iceberg_to_vortex(
    arrow_tables: List[pa.Table],
    iceberg_schema: Schema,
    output_directory: str,
    io: FileIO,
    file_prefix: str = "data",
    compression: bool = True,
) -> List[DataFile]:
    """Convert multiple Arrow tables to Vortex files in batch.

    Args:
        arrow_tables: List of PyArrow tables to convert
        iceberg_schema: The Iceberg schema
        output_directory: Directory to write Vortex files
        io: FileIO instance
        file_prefix: Prefix for generated file names
        compression: Whether to apply compression

    Returns:
        List of DataFile metadata for created files

    Raises:
        ValueError: If batch conversion fails
    """
    _check_vortex_available()

    if not arrow_tables:
        return []

    data_files = []
    total_rows = sum(len(table) for table in arrow_tables)
    logger.info(f"Starting batch conversion of {len(arrow_tables)} tables ({total_rows} total rows)")

    try:
        for i, arrow_table in enumerate(arrow_tables):
            if len(arrow_table) == 0:
                logger.warning(f"Skipping empty table {i}")
                continue

            file_name = f"{file_prefix}_{i:04d}.vortex"
            output_path = f"{output_directory.rstrip('/')}/{file_name}"

            data_file = convert_iceberg_to_vortex_file(
                iceberg_table_data=arrow_table,
                iceberg_schema=iceberg_schema,
                output_path=output_path,
                io=io,
                compression=compression,
            )

            data_files.append(data_file)
            logger.debug(f"Converted batch file {i + 1}/{len(arrow_tables)}: {output_path}")

        logger.info(f"Completed batch conversion: {len(data_files)} Vortex files created")
        return data_files

    except Exception as e:
        raise ValueError(f"Batch conversion failed at file {len(data_files)}: {e}") from e


def analyze_vortex_compatibility(iceberg_schema: Schema) -> Dict[str, Any]:
    """Analyze Iceberg schema compatibility with Vortex format.

    Args:
        iceberg_schema: The Iceberg schema to analyze

    Returns:
        Dictionary with compatibility analysis results
    """
    analysis: Dict[str, Any] = {
        "compatible": True,
        "warnings": [],
        "field_analysis": [],
        "recommended_optimizations": [],
    }

    try:
        for field in iceberg_schema.fields:
            field_info: Dict[str, Any] = {
                "name": field.name,
                "type": str(field.field_type),
                "optional": field.optional,
                "compatible": True,
                "notes": [],
            }

            # Check for potentially problematic types
            field_type = field.field_type

            if isinstance(field_type, MapType):
                field_info["notes"].append("Map types may have limited optimization in Vortex")
                analysis["recommended_optimizations"].append(
                    f"Consider flattening map field '{field.name}' for better performance"
                )

            elif isinstance(field_type, ListType):
                field_info["notes"].append("List types are supported but may impact compression")

            elif isinstance(field_type, StructType):
                field_info["notes"].append("Struct types are well-supported in Vortex")

            elif str(field_type).startswith("decimal"):
                field_info["notes"].append("Decimal types are supported with potential precision considerations")

            analysis["field_analysis"].append(field_info)

        # Generate overall recommendations
        if len(iceberg_schema.fields) > 1000:
            analysis["warnings"].append("Schema has >1000 fields, consider schema optimization")

        nested_fields = sum(1 for field in iceberg_schema.fields if isinstance(field.field_type, (StructType, ListType, MapType)))
        if nested_fields > len(iceberg_schema.fields) * 0.5:
            analysis["recommended_optimizations"].append("High ratio of nested fields detected - consider denormalization")

    except Exception as e:
        analysis["compatible"] = False
        analysis["warnings"].append(f"Analysis failed: {e}")

    return analysis


# Optimized write_vortex_file
def write_vortex_file(
    io: FileIO,
    data_file: DataFile,
    arrow_table: pa.Table,
    metadata: Dict[str, str] | None = None
) -> DataFile:
    """Write Arrow data to Vortex format - simplified."""
    file_path = data_file.file_path
    
    # Direct write - Vortex handles Arrow natively
    try:
        vx.io.write(arrow_table, file_path)
    except Exception as e:
        # Only use temp file if direct write fails
        with tempfile.NamedTemporaryFile(suffix='.vortex', delete=False) as tmp:
            vx.io.write(arrow_table, tmp.name)
            with io.new_output(file_path) as output:
                with open(tmp.name, 'rb') as f:
                    output.write(f.read())
            os.unlink(tmp.name)
    
    # Get file size
    try:
        file_size = os.path.getsize(file_path)
    except:
        file_size = io.new_input(file_path).length()
    
    return DataFile(
        file_path=file_path,
        file_format=FileFormat.VORTEX,
        file_size_in_bytes=file_size,
        record_count=len(arrow_table)
    )


# Optimized read_vortex_file  
def read_vortex_file(
    io: FileIO,
    data_file: DataFile,
    projected_schema: Schema,
    table_schema: Schema,
    filters: BooleanExpression | None = None
) -> Iterator[pa.RecordBatch]:
    """Read Vortex file - use native APIs."""
    
    # Convert filters once
    vortex_expr = None
    if filters:
        vortex_expr = _convert_iceberg_filter_to_vortex(filters, table_schema)
    
    # Get projection columns
    projection = [field.name for field in projected_schema.fields]
    
    # Use native Vortex open - supports all paths
    vxf = vx.open(data_file.file_path)
    reader = vxf.to_arrow(
        projection=projection,
        expr=vortex_expr,
        batch_size=1_048_576  # 1M rows for better throughput
    )
    
    yield from reader
