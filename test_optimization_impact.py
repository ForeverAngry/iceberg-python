#!/usr/bin/env python3
"""Quick test of optimization impact on different dataset sizes."""

import pyarrow as pa
import time
import tempfile
from pyiceberg.io.pyarrow import _write_vortex_file_optimized

def create_test_data(num_rows):
    """Create test data with realistic structure."""
    data = {
        'id': range(num_rows),
        'name': [f'user_{i}' for i in range(num_rows)],
        'score': [i * 0.1 for i in range(num_rows)],
        'category': [f'cat_{i % 10}' for i in range(num_rows)],
        'timestamp': [1000000 + i for i in range(num_rows)],
    }
    return pa.table(data)

def test_optimization_impact():
    """Test optimization impact on different data sizes."""
    print("🧪 Testing API Optimization Impact")
    print("==================================")
    
    test_cases = [
        (50_000, "Small dataset"),
        (500_000, "Medium dataset"),
        (2_000_000, "Large dataset"),
    ]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for num_rows, description in test_cases:
            print(f"\n📊 {description} ({num_rows:,} rows):")
            
            # Create test data
            test_table = create_test_data(num_rows)
            test_path = f"{temp_dir}/test_{num_rows}.vortex"
            
            # Test our optimized write function
            start_time = time.time()
            try:
                file_size = _write_vortex_file_optimized(
                    test_table.to_batches(),
                    test_path,
                    table_schema=test_table.schema
                )
                write_time = time.time() - start_time
                write_rate = num_rows / write_time
                
                print(f"   ✅ Write: {write_time:.2f}s ({write_rate:,.0f} rows/sec)")
                print(f"   📁 Size: {file_size:,} bytes")
                
                # Calculate efficiency metrics
                bytes_per_row = file_size / num_rows
                mb_per_sec = (file_size / (1024 * 1024)) / write_time
                print(f"   📈 Efficiency: {bytes_per_row:.1f} bytes/row, {mb_per_sec:.1f} MB/s")
                
            except Exception as e:
                print(f"   ❌ Failed: {e}")

if __name__ == "__main__":
    test_optimization_impact()
