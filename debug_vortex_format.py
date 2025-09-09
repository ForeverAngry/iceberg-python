#!/usr/bin/env python3

"""
Debug Vortex vs Parquet Implementation
====================================== 

Check what file formats are actually being written and debug performance.
"""

import time
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import numpy as np
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, IntegerType, LongType, StringType, 
    DoubleType, BooleanType, TimestampType, DateType
)
from pyiceberg.expressions import GreaterThan

print("🔍 Debug Vortex vs Parquet Implementation")
print("=" * 50)

def create_test_data(num_rows: int = 10_000):
    """Create a simple test dataset."""
    print(f"📊 Creating {num_rows:,} rows of test data...")
    
    # Generate data
    data = {
        "id": np.arange(1, num_rows + 1, dtype=np.int64),
        "price": np.round(np.random.uniform(10.0, 500.0, num_rows), 2),
        "category": np.random.choice(["Electronics", "Books", "Clothing"], num_rows),
    }
    
    # Create Arrow table with proper schema
    arrow_schema = pa.schema([
        ("id", pa.int64(), False),
        ("price", pa.float64(), False),
        ("category", pa.string(), False),
    ])
    
    return pa.table(data, schema=arrow_schema)

def create_iceberg_schema():
    """Create the Iceberg schema."""
    return Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "price", DoubleType(), required=True),
        NestedField(3, "category", StringType(), required=True),
    )

def debug_table_creation(format_name: str, properties: dict):
    """Debug table creation and file format."""
    print(f"\n🔧 Debugging {format_name}...")
    
    # Create temporary catalog and namespace
    catalog = InMemoryCatalog(name=f"{format_name.lower()}_debug")
    catalog.create_namespace("debug")
    
    # Create schema and test data
    schema = create_iceberg_schema()
    test_data = create_test_data(1000)  # Small dataset for debugging
    
    # Create table with specific properties
    table = catalog.create_table("debug.test_table", schema=schema, properties=properties)
    
    print(f"   Table created with properties: {properties}")
    print(f"   Table format: {table.format_version}")
    print(f"   Table location: {table.location()}")
    
    # Write data
    print(f"   Writing {len(test_data):,} rows...")
    start_time = time.time()
    table.append(test_data)
    write_time = time.time() - start_time
    print(f"   Write completed in {write_time:.3f}s")
    
    # Check what files were actually created
    try:
        table_path = table.location()
        if table_path.startswith("file://"):
            path = Path(table_path[7:])  # Remove file:// prefix
            print(f"   Checking files in: {path}")
            
            if path.exists():
                data_files = []
                metadata_files = []
                
                for file_path in path.rglob("*"):
                    if file_path.is_file():
                        extension = file_path.suffix
                        size = file_path.stat().st_size
                        
                        if extension in ['.parquet', '.vortex']:
                            data_files.append((file_path.name, extension, size))
                        else:
                            metadata_files.append((file_path.name, extension, size))
                
                print(f"   Data files found:")
                for name, ext, size in data_files:
                    print(f"     - {name} ({ext}): {size:,} bytes")
                
                print(f"   Metadata files found:")
                for name, ext, size in metadata_files:
                    print(f"     - {name} ({ext}): {size:,} bytes")
            else:
                print(f"   ❌ Path does not exist: {path}")
        else:
            print(f"   ℹ️  Non-file location: {table_path}")
    except Exception as e:
        print(f"   ⚠️  Could not inspect files: {e}")
    
    # Test read performance
    print(f"   Testing read performance...")
    start_time = time.time()
    result = table.scan().to_arrow()
    read_time = time.time() - start_time
    print(f"   Read {len(result):,} rows in {read_time:.3f}s")
    
    # Test filter performance
    print(f"   Testing filter performance...")
    start_time = time.time()
    filtered = table.scan(row_filter=GreaterThan("price", 100.0)).to_arrow()
    filter_time = time.time() - start_time
    print(f"   Filtered to {len(filtered):,} rows in {filter_time:.3f}s")
    
    return {
        "write_time": write_time,
        "read_time": read_time,
        "filter_time": filter_time,
        "rows": len(test_data),
        "filtered_rows": len(filtered)
    }

def main():
    print("Testing different format configurations...\n")
    
    # Test Parquet (default)
    parquet_results = debug_table_creation("Parquet", {})
    
    # Test Vortex with explicit configuration
    vortex_results = debug_table_creation("Vortex", {"write.format.default": "vortex"})
    
    # Test Vortex with additional properties
    vortex2_results = debug_table_creation("Vortex_Explicit", {
        "write.format.default": "vortex",
        "write.vortex.compression": "default"
    })
    
    print(f"\n📊 SUMMARY:")
    print(f"   Format           Write(ms)  Read(ms)   Filter(ms)")
    print(f"   Parquet:         {parquet_results['write_time']*1000:8.1f} {parquet_results['read_time']*1000:8.1f} {parquet_results['filter_time']*1000:10.1f}")
    print(f"   Vortex:          {vortex_results['write_time']*1000:8.1f} {vortex_results['read_time']*1000:8.1f} {vortex_results['filter_time']*1000:10.1f}")
    print(f"   Vortex_Explicit: {vortex2_results['write_time']*1000:8.1f} {vortex2_results['read_time']*1000:8.1f} {vortex2_results['filter_time']*1000:10.1f}")

if __name__ == "__main__":
    main()
