#!/usr/bin/env python3
"""Simple test to verify our Vortex optimizations are working."""

import tempfile
import time
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.types import IntegerType, StringType, DoubleType
from pyiceberg.schema import Schema

def create_test_data(num_rows=100_000):
    """Create test data with a reasonable size."""
    data = {
        'id': range(num_rows),
        'name': [f'user_{i}' for i in range(num_rows)],
        'score': [i * 0.1 for i in range(num_rows)],
    }
    return pa.table(data)

def main():
    print("🧪 Simple Vortex optimization test")
    print("==================================")
    
    # Create a temporary catalog
    with tempfile.TemporaryDirectory() as temp_dir:
        catalog_props = {
            "type": "in-memory",
        }
        
        catalog = load_catalog("test_catalog", **catalog_props)
        
        # Create test schema
        from pyiceberg.schema import NestedField
        schema = Schema(
            NestedField(1, "id", IntegerType(), required=True),
            NestedField(2, "name", StringType(), required=True),
            NestedField(3, "score", DoubleType(), required=True),
        )
        
        # Create table with Vortex format
        table = catalog.create_table(
            "vortex_optimization_test",
            schema=schema,
            properties={
                "write.format.default": "vortex",
                "write.target-file-size-bytes": "50000000",  # 50MB target
            }
        )
        
        # Test small dataset (should use regular write)
        print("\n📊 Testing small dataset (10k rows)...")
        small_data = create_test_data(10_000)
        start_time = time.time()
        
        table.append(small_data)
        
        small_time = time.time() - start_time
        print(f"   Small dataset write: {small_time:.2f}s ({10_000/small_time:.0f} rows/sec)")
        
        # Test large dataset (should use streaming)
        print("\n📊 Testing large dataset (150k rows)...")
        large_data = create_test_data(150_000)
        start_time = time.time()
        
        table.append(large_data)
        
        large_time = time.time() - start_time
        print(f"   Large dataset write: {large_time:.2f}s ({150_000/large_time:.0f} rows/sec)")
        
        # Test read performance
        print("\n📖 Testing read performance...")
        start_time = time.time()
        
        result = table.scan().to_arrow()
        total_rows = len(result)
        
        read_time = time.time() - start_time
        print(f"   Read performance: {read_time:.2f}s ({total_rows/read_time:.0f} rows/sec)")
        print(f"   Total rows read: {total_rows}")
        
        print("\n✅ Test completed successfully!")

if __name__ == "__main__":
    main()
