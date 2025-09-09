#!/usr/bin/env python3
"""
Comprehensive Vortex vs Parquet Performance Comparison
======================================================

This module provides a unified interface for all Vortex performance testing,
combining optimization analysis, format comparison, and scaling studies.

Features:
- Schema compatibility optimization validation
- API-guided batch sizing analysis
- Comprehensive format comparison (Vortex vs Parquet)
- Multi-scale performance testing (100K to 15M+ rows)
- Production-ready benchmarking

Usage:
    # Quick performance check
    python tests/benchmark/comprehensive_benchmark.py --quick
    
    # Full benchmark suite  
    python tests/benchmark/comprehensive_benchmark.py --full
    
    # Optimization-focused testing
    python tests/benchmark/comprehensive_benchmark.py --optimizations
"""

import argparse
import pyarrow as pa
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Tuple, Optional

try:
    import vortex as vx
    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False
    print("⚠️  Vortex not available - skipping Vortex-specific tests")

from pyiceberg.io.pyarrow import _calculate_optimal_vortex_batch_size, _optimize_vortex_batch_layout


class BenchmarkSuite:
    """Comprehensive benchmark suite for Vortex performance analysis."""
    
    def __init__(self, temp_dir: Optional[str] = None):
        self.temp_dir = temp_dir or tempfile.mkdtemp()
        self.results = {}
    
    def create_realistic_data(self, num_rows: int, complexity: str = "medium") -> pa.Table:
        """Create realistic test data with varying complexity."""
        base_data = {
            'id': range(num_rows),
            'name': [f'user_{i}' for i in range(num_rows)],
            'timestamp': [1000000 + i for i in range(num_rows)],
        }
        
        if complexity == "simple":
            base_data.update({
                'value': [i * 1.1 for i in range(num_rows)],
                'status': ['active' if i % 2 == 0 else 'inactive' for i in range(num_rows)],
            })
        elif complexity == "medium":
            base_data.update({
                'score': [i * 0.1 for i in range(num_rows)],
                'category': [f'cat_{i % 10}' for i in range(num_rows)],
                'value': [i * 1.5 for i in range(num_rows)],
                'status': ['active' if i % 3 == 0 else 'inactive' for i in range(num_rows)],
            })
        elif complexity == "complex":
            base_data.update({
                'score': [i * 0.1 for i in range(num_rows)],
                'category': [f'cat_{i % 20}' for i in range(num_rows)],
                'subcategory': [f'subcat_{i % 100}' for i in range(num_rows)],
                'value': [i * 1.5 for i in range(num_rows)],
                'price': [float(i % 1000) + 0.99 for i in range(num_rows)],
                'quantity': [i % 50 + 1 for i in range(num_rows)],
                'status': ['active' if i % 3 == 0 else 'inactive' for i in range(num_rows)],
                'metadata': [f'{{"key": "value_{i % 10}"}}' for i in range(num_rows)],
            })
        
        return pa.table(base_data)
    
    def benchmark_vortex_write(self, table: pa.Table, optimize: bool = True) -> Tuple[float, int]:
        """Benchmark Vortex write performance."""
        if not VORTEX_AVAILABLE:
            return 0.0, 0
        
        file_path = f"{self.temp_dir}/test_vortex_{int(time.time())}.vortex"
        
        start_time = time.time()
        try:
            if optimize:
                # Use our optimizations
                optimal_batch_size = _calculate_optimal_vortex_batch_size(table)
                if table.num_rows > 100_000:  # Only optimize for larger datasets
                    batches = table.to_batches(max_chunksize=optimal_batch_size)
                    optimized_batches = _optimize_vortex_batch_layout(batches, optimal_batch_size)
                    reader = pa.RecordBatchReader.from_batches(table.schema, optimized_batches)
                else:
                    reader = table.to_reader()
            else:
                # Use default batching
                reader = table.to_reader()
            
            vx.io.write(reader, file_path)
            write_time = time.time() - start_time
            
            # Get file size
            file_size = Path(file_path).stat().st_size
            return write_time, file_size
            
        except Exception as e:
            print(f"   ❌ Vortex write failed: {e}")
            return 0.0, 0
    
    def benchmark_parquet_write(self, table: pa.Table) -> Tuple[float, int]:
        """Benchmark Parquet write performance."""
        file_path = f"{self.temp_dir}/test_parquet_{int(time.time())}.parquet"
        
        start_time = time.time()
        try:
            import pyarrow.parquet as pq
            pq.write_table(table, file_path)
            write_time = time.time() - start_time
            
            # Get file size
            file_size = Path(file_path).stat().st_size
            return write_time, file_size
            
        except Exception as e:
            print(f"   ❌ Parquet write failed: {e}")
            return 0.0, 0
    
    def run_optimization_analysis(self):
        """Run analysis of our optimization strategies."""
        print("🔧 Vortex Optimization Analysis")
        print("================================")
        
        # Test batch size calculation
        print("\n📊 Batch Size Optimization:")
        test_cases = [
            (10_000, "Small"),
            (100_000, "Medium"), 
            (1_000_000, "Large"),
            (10_000_000, "Very Large")
        ]
        
        for num_rows, description in test_cases:
            table = self.create_realistic_data(num_rows)
            optimal_size = _calculate_optimal_vortex_batch_size(table)
            print(f"   {description:>10} ({num_rows:>8,} rows) → {optimal_size:>6,} batch size")
        
        # Test optimization impact
        print("\n🚀 Optimization Impact Analysis:")
        for num_rows in [100_000, 500_000, 1_500_000]:
            table = self.create_realistic_data(num_rows)
            
            if VORTEX_AVAILABLE:
                # Test without optimization
                baseline_time, _ = self.benchmark_vortex_write(table, optimize=False)
                # Test with optimization
                optimized_time, _ = self.benchmark_vortex_write(table, optimize=True)
                
                if baseline_time > 0 and optimized_time > 0:
                    baseline_rate = num_rows / baseline_time
                    optimized_rate = num_rows / optimized_time
                    improvement = (optimized_rate / baseline_rate - 1) * 100
                    
                    print(f"   {num_rows:>8,} rows: {improvement:+.1f}% improvement")
                else:
                    print(f"   {num_rows:>8,} rows: Test failed")
    
    def run_format_comparison(self, dataset_sizes: List[int]):
        """Run comprehensive Vortex vs Parquet comparison."""
        print("\n📈 Format Performance Comparison")
        print("================================")
        
        results = []
        
        for num_rows in dataset_sizes:
            print(f"\n📊 Testing {num_rows:,} rows:")
            
            table = self.create_realistic_data(num_rows, "medium")
            
            # Vortex performance
            if VORTEX_AVAILABLE:
                vortex_time, vortex_size = self.benchmark_vortex_write(table, optimize=True)
                vortex_rate = num_rows / vortex_time if vortex_time > 0 else 0
                print(f"   🔺 Vortex: {vortex_rate:>8,.0f} rows/sec, {vortex_size:>8,} bytes")
            else:
                vortex_rate, vortex_size = 0, 0
                print(f"   🔺 Vortex: Not available")
            
            # Parquet performance
            parquet_time, parquet_size = self.benchmark_parquet_write(table)
            parquet_rate = num_rows / parquet_time if parquet_time > 0 else 0
            print(f"   📦 Parquet: {parquet_rate:>7,.0f} rows/sec, {parquet_size:>8,} bytes")
            
            if vortex_rate > 0 and parquet_rate > 0:
                write_ratio = vortex_rate / parquet_rate
                size_ratio = parquet_size / vortex_size if vortex_size > 0 else 0
                print(f"   📊 Vortex is {write_ratio:.2f}x faster, {size_ratio:.2f}x compression ratio")
                
                results.append({
                    'rows': num_rows,
                    'vortex_rate': vortex_rate,
                    'parquet_rate': parquet_rate,
                    'vortex_size': vortex_size,
                    'parquet_size': parquet_size,
                    'write_ratio': write_ratio,
                    'size_ratio': size_ratio
                })
        
        return results
    
    def run_scaling_analysis(self):
        """Run scaling analysis across different dataset sizes."""
        print("\n📏 Scaling Performance Analysis")
        print("===============================")
        
        sizes = [50_000, 200_000, 800_000, 3_200_000]
        self.run_format_comparison(sizes)
    
    def run_quick_benchmark(self):
        """Run a quick benchmark for development/testing."""
        print("⚡ Quick Benchmark")
        print("=================")
        
        sizes = [100_000, 500_000]
        results = self.run_format_comparison(sizes)
        
        if results:
            print("\n🎯 Quick Summary:")
            for result in results:
                rows = result['rows']
                write_ratio = result.get('write_ratio', 0)
                size_ratio = result.get('size_ratio', 0)
                print(f"   {rows:>7,} rows: {write_ratio:.2f}x write speed, {size_ratio:.2f}x compression")
    
    def run_full_benchmark(self):
        """Run the complete benchmark suite."""
        print("🚀 Full Benchmark Suite")
        print("=======================")
        
        self.run_optimization_analysis()
        
        sizes = [100_000, 500_000, 1_500_000, 5_000_000]
        results = self.run_format_comparison(sizes)
        
        # Generate summary report
        if results:
            print("\n📊 Complete Performance Summary")
            print("=" * 60)
            print(f"{'Dataset Size':<12} {'Vortex (K/s)':<12} {'Parquet (K/s)':<13} {'Speed Ratio':<11} {'Size Ratio'}")
            print("-" * 60)
            
            for result in results:
                rows = result['rows']
                vortex_k = result['vortex_rate'] / 1000
                parquet_k = result['parquet_rate'] / 1000
                write_ratio = result.get('write_ratio', 0)
                size_ratio = result.get('size_ratio', 0)
                
                print(f"{rows/1000:>8.0f}K {vortex_k:>10.0f}K {parquet_k:>11.0f}K {write_ratio:>9.2f}x {size_ratio:>9.2f}x")


def main():
    """Main benchmark runner with CLI interface."""
    parser = argparse.ArgumentParser(description="Vortex Performance Benchmark Suite")
    parser.add_argument("--quick", action="store_true", help="Run quick benchmark")
    parser.add_argument("--full", action="store_true", help="Run full benchmark suite")
    parser.add_argument("--optimizations", action="store_true", help="Run optimization analysis only")
    parser.add_argument("--scaling", action="store_true", help="Run scaling analysis")
    
    args = parser.parse_args()
    
    # Default to quick if no arguments
    if not any([args.quick, args.full, args.optimizations, args.scaling]):
        args.quick = True
    
    print("🎯 Vortex Performance Benchmark Suite")
    print("====================================")
    
    if not VORTEX_AVAILABLE:
        print("⚠️  Warning: Vortex not available. Some tests will be skipped.")
    
    print()
    
    with tempfile.TemporaryDirectory() as temp_dir:
        suite = BenchmarkSuite(temp_dir)
        
        if args.optimizations:
            suite.run_optimization_analysis()
        elif args.scaling:
            suite.run_scaling_analysis()
        elif args.full:
            suite.run_full_benchmark()
        elif args.quick:
            suite.run_quick_benchmark()
    
    print("\n✅ Benchmark complete!")


if __name__ == "__main__":
    main()
