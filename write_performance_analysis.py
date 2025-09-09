#!/usr/bin/env python3
"""
Detailed Write Performance Analysis for Vortex vs Parquet
=========================================================

This script analyzes the write path performance in detail to identify bottlenecks
in the Vortex implementation. It profiles various stages of the write process.
"""

import cProfile
import io
import pstats
import tempfile
import time
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog.memory import InMemoryCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
)

print("🔍 Detailed Vortex Write Performance Analysis")
print("=" * 50)

class WritePerformanceProfiler:
    def __init__(self):
        self.temp_dir = Path(tempfile.mkdtemp(prefix="vortex_write_analysis_"))
        print(f"📁 Using temp directory: {self.temp_dir}")
        self.setup_catalogs()
        
    def setup_catalogs(self):
        """Setup catalogs for both formats."""
        self.vortex_catalog = InMemoryCatalog(name="vortex_analysis")
        self.vortex_catalog.create_namespace("analysis")
        
        self.parquet_catalog = InMemoryCatalog(name="parquet_analysis")
        self.parquet_catalog.create_namespace("analysis")
        
    def generate_test_schema(self) -> Schema:
        """Generate a test schema."""
        return Schema(
            NestedField(1, "id", LongType(), required=True),
            NestedField(2, "user_id", IntegerType(), required=True),
            NestedField(3, "product_name", StringType(), required=True),
            NestedField(4, "category", StringType(), required=True), 
            NestedField(5, "price", DoubleType(), required=True),
            NestedField(6, "quantity", IntegerType(), required=True),
            NestedField(7, "total_amount", DoubleType(), required=True),
            NestedField(8, "is_premium", BooleanType(), required=True),
        )
    
    def generate_test_data(self, num_rows: int = 100_000) -> pa.Table:
        """Generate a moderate-sized test dataset for focused analysis."""
        print(f"🔄 Generating {num_rows:,} rows for analysis...")
        
        # Simpler, more predictable data generation
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
        
        # Create Arrow table with proper schema
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
        
    def profile_write_stages(self, table_data: pa.Table, format_name: str) -> Dict:
        """Profile individual stages of the write process."""
        print(f"\n🔍 Profiling {format_name} write stages...")
        
        # Create table
        schema = self.generate_test_schema()
        
        if format_name == "vortex":
            catalog = self.vortex_catalog
            table_props = {"write.format.default": "vortex"}
        else:
            catalog = self.parquet_catalog
            table_props = {"write.format.default": "parquet"}
            
        table = catalog.create_table(
            identifier=f"analysis.{format_name}_test",
            schema=schema,
            location=str(self.temp_dir / f"{format_name}_table"),
            properties=table_props,
        )
        
        # Stage 1: Table preparation time
        stage1_start = time.perf_counter()
        # (This is minimal for this test)
        stage1_time = time.perf_counter() - stage1_start
        
        # Stage 2: Actual write operation with profiling
        profiler = cProfile.Profile()
        
        stage2_start = time.perf_counter()
        profiler.enable()
        
        table.append(table_data)
        
        profiler.disable()
        stage2_time = time.perf_counter() - stage2_start
        
        # Analyze profiling results
        stats_stream = io.StringIO()
        stats = pstats.Stats(profiler, stream=stats_stream).sort_stats('cumulative')
        stats.print_stats(20)  # Top 20 functions
        
        profiling_output = stats_stream.getvalue()
        
        # Get file sizes
        file_sizes = self.get_table_file_sizes(table)
        total_size = sum(file_sizes.values()) if file_sizes else 0
        
        return {
            "format": format_name,
            "stage1_time": stage1_time,
            "stage2_time": stage2_time,
            "total_time": stage1_time + stage2_time,
            "rows": len(table_data),
            "rows_per_sec": len(table_data) / stage2_time if stage2_time > 0 else 0,
            "total_size": total_size,
            "size_mb": total_size / (1024 * 1024) if total_size > 0 else 0,
            "profiling_output": profiling_output,
            "file_sizes": file_sizes
        }
        
    def get_table_file_sizes(self, table) -> Dict[str, int]:
        """Get file sizes for all files in the table."""
        file_sizes = {}
        try:
            # Get table metadata to find data files
            metadata = table.metadata
            if hasattr(metadata, 'snapshots') and metadata.snapshots:
                latest_snapshot = metadata.snapshots[-1]
                if hasattr(latest_snapshot, 'manifests'):
                    for manifest in latest_snapshot.manifests():
                        for entry in manifest.fetch_manifest_entry():
                            data_file = entry.data_file
                            try:
                                file_path = data_file.file_path
                                if Path(file_path).exists():
                                    file_sizes[file_path] = Path(file_path).stat().st_size
                                else:
                                    # Try relative to table location
                                    table_path = Path(table.location()) / Path(file_path).name
                                    if table_path.exists():
                                        file_sizes[file_path] = table_path.stat().st_size
                            except Exception as e:
                                print(f"Warning: Could not get size for {data_file.file_path}: {e}")
        except Exception as e:
            print(f"Warning: Could not analyze table files: {e}")
            
        return file_sizes
        
    def analyze_vortex_write_bottlenecks(self, table_data: pa.Table):
        """Deep dive into Vortex write bottlenecks."""
        print(f"\n🔍 Deep Analysis: Vortex Write Bottlenecks")
        print("-" * 40)
        
        # Test different aspects of Vortex writing
        from pyiceberg.io.vortex import write_vortex_file
        from pyiceberg.io.pyarrow import PyArrowFileIO
        
        io = PyArrowFileIO()
        
        # Test 1: Direct vortex write (bypass table operations)
        print("Test 1: Direct Vortex file write")
        direct_file = self.temp_dir / "direct_vortex_test.vortex"
        
        direct_start = time.perf_counter()
        try:
            file_size = write_vortex_file(table_data, str(direct_file), io)
            direct_time = time.perf_counter() - direct_start
            direct_speed = len(table_data) / direct_time
            print(f"   ✅ Direct write: {direct_time:.3f}s, {direct_speed:,.0f} rows/sec, {file_size / 1024 / 1024:.1f} MB")
        except Exception as e:
            print(f"   ❌ Direct write failed: {e}")
            direct_time = float('inf')
            direct_speed = 0
            
        # Test 2: Vortex with different chunk sizes
        print("\nTest 2: Vortex write with different data chunking")
        
        chunk_sizes = [10_000, 50_000, 100_000]
        for chunk_size in chunk_sizes:
            if len(table_data) <= chunk_size:
                continue
                
            chunk_file = self.temp_dir / f"chunk_{chunk_size}_vortex.vortex"
            
            chunk_start = time.perf_counter()
            try:
                # Split data into chunks and write
                chunks = []
                for i in range(0, len(table_data), chunk_size):
                    chunk = table_data.slice(i, min(chunk_size, len(table_data) - i))
                    chunks.append(chunk)
                
                combined_table = pa.concat_tables(chunks)
                file_size = write_vortex_file(combined_table, str(chunk_file), io)
                chunk_time = time.perf_counter() - chunk_start
                chunk_speed = len(table_data) / chunk_time
                print(f"   Chunk size {chunk_size:,}: {chunk_time:.3f}s, {chunk_speed:,.0f} rows/sec")
            except Exception as e:
                print(f"   Chunk size {chunk_size:,}: Failed - {e}")
                
        # Test 3: Compare temp file vs direct write performance
        print("\nTest 3: Temp file vs Direct write comparison")
        
        from pyiceberg.io.vortex import _write_vortex_direct, _write_vortex_temp_file, _can_use_direct_streaming
        
        local_file = self.temp_dir / "local_direct.vortex"
        remote_like_file = "s3://bucket/remote_file.vortex"  # This will trigger temp file logic
        
        # Direct write test
        direct_start = time.perf_counter()
        try:
            if _can_use_direct_streaming(str(local_file), io):
                size1 = _write_vortex_direct(table_data, str(local_file), io)
                direct_time = time.perf_counter() - direct_start
                print(f"   Direct write: {direct_time:.3f}s, {len(table_data) / direct_time:,.0f} rows/sec")
            else:
                print("   Direct write not supported for this path")
        except Exception as e:
            print(f"   Direct write failed: {e}")
            
        # Temp file write test
        temp_file = self.temp_dir / "temp_file_test.vortex"
        temp_start = time.perf_counter()
        try:
            size2 = _write_vortex_temp_file(table_data, str(temp_file), io)
            temp_time = time.perf_counter() - temp_start
            print(f"   Temp file write: {temp_time:.3f}s, {len(table_data) / temp_time:,.0f} rows/sec")
        except Exception as e:
            print(f"   Temp file write failed: {e}")
            
    def run_analysis(self):
        """Run the complete write performance analysis."""
        # Generate test data
        test_data = self.generate_test_data(100_000)  # 100k rows for focused analysis
        
        # Profile both formats
        vortex_results = self.profile_write_stages(test_data, "vortex")
        parquet_results = self.profile_write_stages(test_data, "parquet")
        
        # Detailed Vortex analysis
        self.analyze_vortex_write_bottlenecks(test_data)
        
        # Compare results
        print(f"\n📊 WRITE PERFORMANCE COMPARISON")
        print("=" * 50)
        print(f"Dataset: {len(test_data):,} rows, {test_data.nbytes / 1024 / 1024:.1f} MB")
        print()
        
        print(f"VORTEX:")
        print(f"   Time: {vortex_results['total_time']:.3f}s")
        print(f"   Speed: {vortex_results['rows_per_sec']:,.0f} rows/sec")
        print(f"   Size: {vortex_results['size_mb']:.1f} MB")
        print()
        
        print(f"PARQUET:")
        print(f"   Time: {parquet_results['total_time']:.3f}s")
        print(f"   Speed: {parquet_results['rows_per_sec']:,.0f} rows/sec")
        print(f"   Size: {parquet_results['size_mb']:.1f} MB")
        print()
        
        if parquet_results['total_time'] > 0:
            speedup = parquet_results['total_time'] / vortex_results['total_time']
            print(f"VORTEX SPEEDUP: {speedup:.2f}x vs Parquet")
        
        # Show profiling details for bottlenecks
        print(f"\n🔍 VORTEX PROFILING DETAILS:")
        print("-" * 30)
        print(vortex_results['profiling_output'][:2000])  # First 2000 chars
        
        print(f"\n🔍 PARQUET PROFILING DETAILS:")
        print("-" * 30)
        print(parquet_results['profiling_output'][:2000])  # First 2000 chars
        
        return {
            'vortex': vortex_results,
            'parquet': parquet_results
        }


if __name__ == "__main__":
    profiler = WritePerformanceProfiler()
    try:
        results = profiler.run_analysis()
        print(f"\n✅ Analysis complete. Results saved in {profiler.temp_dir}")
    except Exception as e:
        print(f"❌ Analysis failed: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup
        import shutil
        try:
            shutil.rmtree(profiler.temp_dir)
            print(f"🧹 Cleaned up {profiler.temp_dir}")
        except Exception as e:
            print(f"Warning: Could not cleanup {profiler.temp_dir}: {e}")
