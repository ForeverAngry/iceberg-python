#!/usr/bin/env python3
"""Test the specific impact of batch size optimizations on Vortex write performance."""

import pyarrow as pa
import time
import tempfile
import vortex as vx
from pyiceberg.io.pyarrow import _calculate_optimal_vortex_batch_size, _optimize_vortex_batch_layout

def create_test_data(num_rows):
    """Create test data with realistic structure."""
    data = {
        'id': range(num_rows),
        'name': [f'user_{i}' for i in range(num_rows)],
        'score': [i * 0.1 for i in range(num_rows)],
        'category': [f'cat_{i % 10}' for i in range(num_rows)],
        'value': [i * 1.5 for i in range(num_rows)],
        'status': ['active' if i % 3 == 0 else 'inactive' for i in range(num_rows)],
    }
    return pa.table(data)

def test_without_batch_optimization(table, file_path):
    """Test write performance without batch optimization (baseline)."""
    start_time = time.time()
    
    # Direct write with default batching
    reader = table.to_reader()
    vx.io.write(reader, file_path)
    
    write_time = time.time() - start_time
    return write_time, table.num_rows / write_time

def test_with_batch_optimization(table, file_path):
    """Test write performance with our batch size optimizations."""
    start_time = time.time()
    
    # Apply our optimizations
    optimal_batch_size = _calculate_optimal_vortex_batch_size(table)
    
    # Create batches with optimal size
    batches = table.to_batches(max_chunksize=optimal_batch_size)
    
    # Optimize batch layout
    optimized_batches = _optimize_vortex_batch_layout(batches, optimal_batch_size)
    
    # Write with optimized batches
    reader = pa.RecordBatchReader.from_batches(table.schema, optimized_batches)
    vx.io.write(reader, file_path)
    
    write_time = time.time() - start_time
    return write_time, table.num_rows / write_time, optimal_batch_size

def run_batch_optimization_test():
    """Run comprehensive batch optimization tests."""
    print("🧪 Batch Size Optimization Performance Test")
    print("===========================================")
    
    test_cases = [
        (100_000, "Small dataset"),
        (500_000, "Medium dataset"), 
        (1_500_000, "Large dataset"),
        (3_000_000, "Very large dataset"),
    ]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        for num_rows, description in test_cases:
            print(f"\n📊 {description} ({num_rows:,} rows):")
            
            # Create test data
            table = create_test_data(num_rows)
            
            # Test baseline (without optimization)
            baseline_path = f"{temp_dir}/baseline_{num_rows}.vortex"
            try:
                baseline_time, baseline_rate = test_without_batch_optimization(table, baseline_path)
                print(f"   📋 Baseline: {baseline_time:.2f}s ({baseline_rate:,.0f} rows/sec)")
            except Exception as e:
                print(f"   ❌ Baseline failed: {e}")
                continue
            
            # Test with optimization
            optimized_path = f"{temp_dir}/optimized_{num_rows}.vortex"
            try:
                opt_time, opt_rate, batch_size = test_with_batch_optimization(table, optimized_path)
                print(f"   🚀 Optimized: {opt_time:.2f}s ({opt_rate:,.0f} rows/sec)")
                print(f"   ⚙️  Batch size: {batch_size:,} rows")
                
                # Calculate improvement
                if baseline_rate and opt_rate:
                    improvement = (opt_rate / baseline_rate - 1) * 100
                    speedup = opt_rate / baseline_rate
                    print(f"   📈 Performance: {improvement:+.1f}% ({speedup:.2f}x)")
                    
                    # Time comparison
                    time_saved = baseline_time - opt_time
                    print(f"   ⏱️  Time saved: {time_saved:+.2f}s")
                
            except Exception as e:
                print(f"   ❌ Optimized failed: {e}")

def run_batch_size_scaling_test():
    """Test how different batch sizes affect performance."""
    print("\n🔬 Batch Size Scaling Analysis")
    print("==============================")
    
    # Use a medium-sized dataset for this test
    num_rows = 800_000
    table = create_test_data(num_rows)
    
    # Test different batch sizes
    batch_sizes = [10_000, 25_000, 50_000, 100_000, 200_000, 400_000]
    
    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Testing {num_rows:,} rows with different batch sizes:")
        
        results = []
        for batch_size in batch_sizes:
            file_path = f"{temp_dir}/batch_{batch_size}.vortex"
            
            try:
                start_time = time.time()
                
                # Create batches with specific size
                batches = table.to_batches(max_chunksize=batch_size)
                reader = pa.RecordBatchReader.from_batches(table.schema, batches)
                vx.io.write(reader, file_path)
                
                write_time = time.time() - start_time
                rate = num_rows / write_time
                
                results.append((batch_size, rate, write_time))
                print(f"   {batch_size:>6,} batch size: {rate:>8,.0f} rows/sec ({write_time:.2f}s)")
                
            except Exception as e:
                print(f"   {batch_size:>6,} batch size: Failed ({e})")
        
        if results:
            # Find best performing batch size
            best_batch, best_rate, best_time = max(results, key=lambda x: x[1])
            print(f"\n   🏆 Best performance: {best_batch:,} batch size ({best_rate:,.0f} rows/sec)")
            
            # Compare with our optimization
            optimal_batch = _calculate_optimal_vortex_batch_size(table)
            print(f"   🎯 Our optimization suggests: {optimal_batch:,} batch size")

if __name__ == "__main__":
    run_batch_optimization_test()
    run_batch_size_scaling_test()
