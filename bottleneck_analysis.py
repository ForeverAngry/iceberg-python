#!/usr/bin/env python3
"""
Write Path Bottleneck Analysis
==============================

Test individual components of the write path to identify the specific bottleneck.
"""

import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

def generate_test_data(num_rows: int = 50_000) -> pa.Table:
    """Generate test data."""
    data = {
        "id": np.arange(1, num_rows + 1, dtype=np.int64),
        "user_id": np.random.randint(1, 10_000, num_rows, dtype=np.int32),
        "product_name": [f"Product_{i % 1000:04d}" for i in range(num_rows)],
        "category": np.random.choice(["Electronics", "Books", "Clothing"], num_rows),
        "price": np.round(np.random.uniform(10.0, 1000.0, num_rows), 2),
        "quantity": np.random.randint(1, 10, num_rows, dtype=np.int32),
        "is_premium": np.random.choice([True, False], num_rows, p=[0.2, 0.8]),
    }
    
    data["total_amount"] = np.round(data["price"] * data["quantity"], 2)
    
    arrow_schema = pa.schema([
        ("id", pa.int64(), False),
        ("user_id", pa.int32(), False),
        ("product_name", pa.string(), False),
        ("category", pa.string(), False),
        ("price", pa.float64(), False),
        ("quantity", pa.int32(), False),
        ("total_amount", pa.float64(), False),
        ("is_premium", pa.bool_(), False),
    ])
    
    return pa.Table.from_pydict(data, schema=arrow_schema)

def benchmark_write_components():
    """Test individual components of the write path."""
    print("🔍 Write Path Component Analysis")
    print("=" * 40)
    
    test_data = generate_test_data(50_000)
    print(f"Generated test data: {len(test_data):,} rows, {test_data.nbytes / 1024 / 1024:.1f} MB")
    
    # Test 1: Raw Vortex API write speed
    print("\n1. Raw Vortex API Speed Test")
    from pyiceberg.io.vortex import write_vortex_file
    from pyiceberg.io.pyarrow import PyArrowFileIO
    
    temp_dir = Path(tempfile.mkdtemp(prefix="component_test_"))
    io = PyArrowFileIO()
    
    try:
        vortex_file = temp_dir / "raw_test.vortex"
        
        start = time.perf_counter()
        file_size = write_vortex_file(test_data, str(vortex_file), io)
        raw_time = time.perf_counter() - start
        raw_speed = len(test_data) / raw_time
        
        print(f"   Raw Vortex write: {raw_time:.3f}s, {raw_speed:,.0f} rows/sec")
        
        # Test 2: Test schema conversion overhead
        print("\n2. Schema Conversion Overhead")
        from pyiceberg.io.pyarrow import pyarrow_to_schema, schema_to_pyarrow
        
        schema = Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "user_id", IntegerType(), required=True),
            NestedField(3, "product_name", StringType(), required=True),
            NestedField(4, "category", StringType(), required=True), 
            NestedField(5, "price", DoubleType(), required=True),
            NestedField(6, "quantity", IntegerType(), required=True),
            NestedField(7, "total_amount", DoubleType(), required=True),
            NestedField(8, "is_premium", BooleanType(), required=True),
        )
        
        start = time.perf_counter()
        for _ in range(10):  # Multiple iterations to see overhead
            # Use the known schema since test_data doesn't have field IDs
            arrow_schema = schema_to_pyarrow(schema, include_field_ids=True)
        schema_time = time.perf_counter() - start
        
        # Also test task_schema creation which is used in the write path
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
        start2 = time.perf_counter()
        for _ in range(10):
            task_schema = _pyarrow_to_schema_without_ids(test_data.schema)
        task_schema_time = time.perf_counter() - start2
        
        print(f"   Schema conversions (10x): {schema_time:.3f}s ({schema_time/10*1000:.1f}ms each)")
        
        # Test 3: Test _to_requested_schema overhead
        print("\n3. Schema Transformation Overhead")
        from pyiceberg.io.pyarrow import _to_requested_schema
        
        # Convert to batches and test transformation
        batches = test_data.to_batches(max_chunksize=10000)  # 5 batches
        
        # Use a simple task schema that matches our data
        from pyiceberg.io.pyarrow import _pyarrow_to_schema_without_ids
        task_schema = _pyarrow_to_schema_without_ids(test_data.schema)
        
        start = time.perf_counter()
        transformed_batches = [
            _to_requested_schema(
                requested_schema=schema,
                file_schema=task_schema,
                batch=batch,
                downcast_ns_timestamp_to_us=False,
                include_field_ids=True,
            )
            for batch in batches
        ]
        transform_time = time.perf_counter() - start
        
        print(f"   Schema transformations ({len(batches)} batches): {transform_time:.3f}s")
        
        # Test 4: Test WriteTask creation and processing
        print("\n4. WriteTask Processing Overhead")
        import uuid
        from pyiceberg.table import WriteTask
        
        task = WriteTask(
            write_uuid=uuid.uuid4(),
            task_id=1,
            schema=task_schema,
            record_batches=batches,
            partition_key=None,
        )
        
        start = time.perf_counter()
        # Simulate the write_data_file processing
        arrow_table = pa.Table.from_batches(transformed_batches)
        writetask_time = time.perf_counter() - start
        
        print(f"   WriteTask processing: {writetask_time:.3f}s")
        
        # Test 5: Test full _dataframe_to_data_files path
        print("\n5. Full DataFiles Generation")
        from pyiceberg.io.pyarrow import _dataframe_to_data_files
        
        # Create minimal table metadata
        catalog = InMemoryCatalog(name="test")
        catalog.create_namespace("test")
        
        table = catalog.create_table(
            identifier="test.vortex_component_test",
            schema=schema,
            location=str(temp_dir / "component_table"),
            properties={"write.format.default": "vortex"},
        )
        
        start = time.perf_counter()
        data_files = list(_dataframe_to_data_files(
            table_metadata=table.metadata,
            df=test_data,
            io=io,
        ))
        datafiles_time = time.perf_counter() - start
        
        print(f"   Full datafiles generation: {datafiles_time:.3f}s")
        print(f"   Generated {len(data_files)} data files")
        
        # Analysis Summary
        print(f"\n📊 OVERHEAD ANALYSIS:")
        print(f"   Raw Vortex write:     {raw_time:.3f}s (baseline)")
        print(f"   Schema conversions:    {schema_time/10:.3f}s per operation")
        print(f"   Schema transforms:     {transform_time:.3f}s")
        print(f"   WriteTask processing:  {writetask_time:.3f}s")
        print(f"   Full datafiles path:   {datafiles_time:.3f}s")
        print(f"   Total computed overhead: {(schema_time/10 + transform_time + writetask_time):.3f}s")
        print(f"   Expected vs actual: {raw_time + schema_time/10 + transform_time + writetask_time:.3f}s vs {datafiles_time:.3f}s")
        
        overhead_ratio = datafiles_time / raw_time
        print(f"   Overhead ratio: {overhead_ratio:.2f}x")
        
        # Find the biggest bottleneck
        bottlenecks = [
            ("Raw Vortex", raw_time),
            ("Schema transforms", transform_time),
            ("WriteTask processing", writetask_time),
            ("Other overhead", datafiles_time - raw_time - transform_time - writetask_time)
        ]
        
        print(f"\n🎯 BOTTLENECK BREAKDOWN:")
        for name, time_val in sorted(bottlenecks, key=lambda x: x[1], reverse=True):
            percentage = (time_val / datafiles_time) * 100
            print(f"   {name}: {time_val:.3f}s ({percentage:.1f}%)")
        
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    benchmark_write_components()
