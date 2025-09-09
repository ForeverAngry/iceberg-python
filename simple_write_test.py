#!/usr/bin/env python3
"""
Simple Write Performance Test to Identify Vortex Bottlenecks
============================================================
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

print("🔍 Simple Vortex Write Performance Test")
print("=" * 40)

def generate_test_data(num_rows: int = 50_000) -> pa.Table:
    """Generate test data."""
    print(f"🔄 Generating {num_rows:,} rows...")
    
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
    
    table = pa.Table.from_pydict(data, schema=arrow_schema)
    print(f"✅ Generated table: {len(table):,} rows, {table.nbytes / 1024 / 1024:.1f} MB")
    return table

def test_direct_vortex_write(table_data: pa.Table):
    """Test direct Vortex file writing."""
    print("\n🔍 Testing Direct Vortex Write...")
    
    from pyiceberg.io.vortex import write_vortex_file
    from pyiceberg.io.pyarrow import PyArrowFileIO
    
    temp_dir = Path(tempfile.mkdtemp(prefix="vortex_test_"))
    io = PyArrowFileIO()
    
    try:
        vortex_file = temp_dir / "test.vortex"
        
        start_time = time.perf_counter()
        file_size = write_vortex_file(table_data, str(vortex_file), io)
        write_time = time.perf_counter() - start_time
        
        rows_per_sec = len(table_data) / write_time if write_time > 0 else 0
        
        print(f"   ✅ Direct Vortex write:")
        print(f"      Time: {write_time:.3f}s")
        print(f"      Speed: {rows_per_sec:,.0f} rows/sec")
        print(f"      File size: {file_size / 1024 / 1024:.1f} MB")
        
        return write_time, rows_per_sec, file_size
        
    except Exception as e:
        print(f"   ❌ Direct Vortex write failed: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

def test_table_append_performance():
    """Test table.append() performance for both formats."""
    print("\n🔍 Testing Table Append Performance...")
    
    # Generate test data
    test_data = generate_test_data(50_000)
    
    temp_dir = Path(tempfile.mkdtemp(prefix="table_test_"))
    
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
    
    try:
        # Test Vortex
        print("   Testing Vortex table append...")
        vortex_catalog = InMemoryCatalog(name="vortex_test")
        vortex_catalog.create_namespace("test")
        
        vortex_table = vortex_catalog.create_table(
            identifier="test.vortex_table",
            schema=schema,
            location=str(temp_dir / "vortex_table"),
            properties={"write.format.default": "vortex"},
        )
        
        vortex_start = time.perf_counter()
        vortex_table.append(test_data)
        vortex_time = time.perf_counter() - vortex_start
        vortex_speed = len(test_data) / vortex_time if vortex_time > 0 else 0
        
        print(f"      Vortex: {vortex_time:.3f}s, {vortex_speed:,.0f} rows/sec")
        
        # Test Parquet
        print("   Testing Parquet table append...")
        parquet_catalog = InMemoryCatalog(name="parquet_test")
        parquet_catalog.create_namespace("test")
        
        parquet_table = parquet_catalog.create_table(
            identifier="test.parquet_table",
            schema=schema,
            location=str(temp_dir / "parquet_table"),
            properties={"write.format.default": "parquet"},
        )
        
        parquet_start = time.perf_counter()
        parquet_table.append(test_data)
        parquet_time = time.perf_counter() - parquet_start
        parquet_speed = len(test_data) / parquet_time if parquet_time > 0 else 0
        
        print(f"      Parquet: {parquet_time:.3f}s, {parquet_speed:,.0f} rows/sec")
        
        if parquet_time > 0 and vortex_time > 0:
            speedup = parquet_time / vortex_time
            print(f"      Vortex speedup: {speedup:.2f}x")
        
        return {
            'vortex_time': vortex_time,
            'vortex_speed': vortex_speed,
            'parquet_time': parquet_time,
            'parquet_speed': parquet_speed
        }
        
    except Exception as e:
        print(f"   ❌ Table append test failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

def analyze_write_stages():
    """Analyze different stages of the write process."""
    print("\n🔍 Analyzing Write Process Stages...")
    
    test_data = generate_test_data(25_000)  # Smaller for detailed analysis
    
    # Test 1: Just direct Vortex write
    direct_result = test_direct_vortex_write(test_data)
    
    # Test 2: Table append (which includes more overhead)
    table_results = test_table_append_performance()
    
    print(f"\n📊 SUMMARY:")
    print(f"   Direct Vortex write: {direct_result[0]:.3f}s, {direct_result[1]:,.0f} rows/sec")
    if table_results:
        print(f"   Vortex table.append(): {table_results['vortex_time']:.3f}s, {table_results['vortex_speed']:,.0f} rows/sec")
        print(f"   Parquet table.append(): {table_results['parquet_time']:.3f}s, {table_results['parquet_speed']:,.0f} rows/sec")
        
        if direct_result[0] and table_results['vortex_time']:
            overhead = table_results['vortex_time'] / direct_result[0]
            print(f"   Vortex table overhead: {overhead:.2f}x vs direct write")

if __name__ == "__main__":
    analyze_write_stages()
