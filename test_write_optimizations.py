#!/usr/bin/env python3
"""Direct test of our Vortex write optimizations."""

import pyarrow as pa
import time
import tempfile
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType, DoubleType
from pyiceberg.table import WriteTask, TableMetadata, MetadataLogEntry, Snapshot, SnapshotLog
from pyiceberg.partitioning import PartitionSpec

def test_write_optimizations():
    print("🔧 Testing Vortex write optimizations directly")
    print("==============================================")
    
    # Create test data
    data = {
        'id': range(100_000),
        'name': [f'user_{i}' for i in range(100_000)],
        'score': [i * 0.1 for i in range(100_000)],
    }
    arrow_table = pa.table(data)
    
    # Create schema
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=True),
        NestedField(2, "name", StringType(), required=True),
        NestedField(3, "score", DoubleType(), required=True),
    )
    
    # Create minimal table metadata
    partition_spec = PartitionSpec()
    
    metadata_log = [MetadataLogEntry(
        metadata_file="metadata.json",
        timestamp_ms=int(time.time() * 1000)
    )]
    
    snapshots = []
    snapshot_log = []
    
    table_metadata = TableMetadata(
        format_version=2,
        table_uuid="test-uuid",
        location="test://location",
        last_sequence_number=0,
        last_updated_ms=int(time.time() * 1000),
        last_column_id=3,
        schema=schema,
        schemas=[schema],
        partition_spec=partition_spec,
        partition_specs=[partition_spec],
        default_spec_id=0,
        last_partition_id=999,
        properties={"write.format.default": "vortex"},
        current_schema_id=0,
        snapshots=snapshots,
        snapshot_log=snapshot_log,
        metadata_log=metadata_log,
        sort_orders=[],
        default_sort_order_id=0,
    )
    
    # Create write task
    write_task = WriteTask(
        write_uuid="test-write-uuid",
        task_id=1,
        record_batches=[arrow_table.to_batches()[0]],
        schema=schema,
    )
    
    # Test the optimized write function
    print("📝 Testing optimized Vortex write...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        io = PyArrowFileIO(properties={"warehouse": temp_dir})
        
        start_time = time.time()
        
        # This should trigger our optimized write path
        try:
            from pyiceberg.io.pyarrow import _write_vortex_file_optimized
            from pyiceberg.catalog import LOCATION_PROVIDERS
            
            # Simple location provider for testing
            class TestLocationProvider:
                def new_data_location(self, data_file_name, partition_key=None):
                    return f"{temp_dir}/{data_file_name}"
            
            location_provider = TestLocationProvider()
            
            data_file = _write_vortex_file_optimized(
                task=write_task,
                file_schema=schema,
                table_metadata=table_metadata,
                io=io,
                location_provider=location_provider,
                downcast_ns_timestamp_to_us=False,
            )
            
            write_time = time.time() - start_time
            
            print(f"✅ Optimized write completed!")
            print(f"   Time: {write_time:.3f}s")
            print(f"   Rate: {100_000/write_time:.0f} rows/sec")
            print(f"   File: {data_file.file_path}")
            print(f"   Size: {data_file.file_size_in_bytes} bytes")
            print(f"   Records: {data_file.record_count}")
            
        except Exception as e:
            print(f"❌ Error testing optimized write: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    test_write_optimizations()
