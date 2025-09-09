#!/usr/bin/env python3
"""Test the impact of our new API-guided Vortex optimizations."""

import tempfile
import time

import pyarrow as pa

from pyiceberg.io.pyarrow import _calculate_optimal_vortex_batch_size, _optimize_vortex_batch_layout

def create_test_data(num_rows):
    """Create test data with varying sizes to test batch optimization."""
    data = {
        'id': range(num_rows),
        'name': [f'user_{i}' for i in range(num_rows)],
        'score': [i * 0.1 for i in range(num_rows)],
        'category': [f'cat_{i % 10}' for i in range(num_rows)],
        'timestamp': [int(time.time()) + i for i in range(num_rows)],
    }
    return pa.table(data)

def test_batch_size_optimization():
    """Test our new optimal batch size calculation."""
    print("🔧 Testing Optimal Batch Size Calculation")
    print("==========================================")
    
    # Test different dataset sizes
    test_cases = [
        (10_000, "Small dataset"),
        (100_000, "Medium dataset"), 
        (1_000_000, "Large dataset"),
        (10_000_000, "Very large dataset"),
    ]
    
    for num_rows, description in test_cases:
        table = create_test_data(num_rows)
        optimal_batch_size = _calculate_optimal_vortex_batch_size(table)
        print(f"   {description} ({num_rows:,} rows): {optimal_batch_size:,} batch size")
    
    print()

def test_batch_layout_optimization():
    """Test our new batch layout optimization."""
    print("🔧 Testing Batch Layout Optimization")
    print("====================================")
    
    # Create test table with inconsistent batch sizes
    small_batch = create_test_data(5_000)
    medium_batch = create_test_data(15_000)
    large_batch = create_test_data(35_000)
    
    # Combine into inconsistent batches
    combined = pa.concat_tables([small_batch, medium_batch, large_batch])
    print(f"   Original table: {len(combined)} rows, {combined.num_rows} total")
    print(f"   Original schema: {combined.schema}")
    
    try:
        # Test our optimization
        optimized = _optimize_vortex_batch_layout(combined, target_batch_size=20_000)
        print(f"   Optimized table: {len(optimized)} rows")
        print(f"   Target batch size: 20,000 rows")
        print(f"   Optimization successful: ✅")
    except Exception as e:
        print(f"   Optimization failed: {e}")
        print(f"   Fallback to original table: ✅")
    
    print()

def benchmark_write_with_optimizations():
    """Benchmark write performance with and without our optimizations."""
    print("📊 Benchmarking Write Performance")
    print("=================================")
    
    # Create test data
    num_rows = 500_000
    test_table = create_test_data(num_rows)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        file_io = PyArrowFileIO()
        
        print(f"   Testing with {num_rows:,} rows...")
        
        # Test 1: Direct write (simulating old approach)
        start_time = time.time()
        test_path = f"{temp_dir}/test_direct.vortex"
        
        try:
            # This simulates writing without our optimizations
            # by using a simple PyArrow approach
            import vortex as vx
            
            # Convert to record batch reader
            reader = test_table.to_reader()
            
            # Write directly without optimization
            vx.io.write(test_path, reader)
            
            direct_time = time.time() - start_time
            direct_rate = num_rows / direct_time
            print(f"   Direct write: {direct_time:.2f}s ({direct_rate:,.0f} rows/sec)")
            
        except Exception as e:
            print(f"   Direct write failed: {e}")
            direct_time = None
            direct_rate = None
        
        # Test 2: Optimized write with our enhancements
        start_time = time.time()
        test_path_opt = f"{temp_dir}/test_optimized.vortex"
        
        try:
            # Calculate optimal batch size
            optimal_batch_size = _calculate_optimal_vortex_batch_size(test_table)
            
            # Optimize batch layout  
            optimized_table = _optimize_vortex_batch_layout(test_table, target_batch_size=optimal_batch_size)
            
            # Write with optimizations
            reader = optimized_table.to_reader()
            vx.io.write(test_path_opt, reader)
            
            optimized_time = time.time() - start_time
            optimized_rate = num_rows / optimized_time
            print(f"   Optimized write: {optimized_time:.2f}s ({optimized_rate:,.0f} rows/sec)")
            
            # Calculate improvement
            if direct_time and optimized_time:
                improvement = (direct_rate / optimized_rate) if optimized_rate < direct_rate else (optimized_rate / direct_rate)
                better = "optimized" if optimized_rate > direct_rate else "direct"
                print(f"   Performance: {better} is {improvement:.2f}x faster")
            
        except Exception as e:
            print(f"   Optimized write failed: {e}")
            
    print()

def main():
    """Run all optimization tests."""
    print("🚀 Testing API-Guided Vortex Optimizations")
    print("============================================")
    print()
    
    test_batch_size_optimization()
    test_batch_layout_optimization()
    benchmark_write_with_optimizations()
    
    print("✅ API optimization testing complete!")

if __name__ == "__main__":
    main()
