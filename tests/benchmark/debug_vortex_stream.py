
import pyarrow as pa
import numpy as np

try:
    import vortex as vx
    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False

def create_test_table(num_rows=100):
    """Creates a simple PyArrow table for testing."""
    data = {
        'id': pa.array(np.arange(num_rows, dtype=np.int64)),
        'name': pa.array([f'user_{i}' if i % 5 != 0 else None for i in range(num_rows)], type=pa.string()),
        'score': pa.array(np.random.rand(num_rows) * 100),
        'is_active': pa.array(np.random.choice([True, False], size=num_rows))
    }
    return pa.table(data)

def debug_streaming_conversion():
    """
    Isolates and debugs the conversion of a PyArrow RecordBatch to a Vortex Array.
    """
    if not VORTEX_AVAILABLE:
        print("Vortex is not installed. Skipping debug script.")
        return

    print("--- Vortex Streaming Conversion Debugger ---")
    
    # 1. Create a sample PyArrow Table
    table = create_test_table(num_rows=200)
    print(f"Created a test table with {table.num_rows} rows and schema:")
    print(table.schema)
    print("-" * 50)

    # 2. Get a RecordBatchReader
    reader = table.to_reader()
    print("Created a RecordBatchReader from the table.")
    print("-" * 50)

    # 3. Iterate and attempt conversion
    batch_num = 0
    for batch in reader:
        batch_num += 1
        print(f"Processing Batch #{batch_num}")
        print(f"  - Batch rows: {batch.num_rows}")
        print(f"  - Batch schema:\n{batch.schema}")
        
        try:
            # This is the call that fails in the benchmark
            # vortex_array = vx.array(batch)

            # New approach: Convert column by column
            vortex_columns = {}
            for i, pa_array in enumerate(batch.columns):
                col_name = batch.schema.field(i).name
                print(f"    - Converting column '{col_name}'...")
                try:
                    vortex_columns[col_name] = vx.array(pa_array)
                    print(f"      ✅ Converted column '{col_name}' successfully.")
                except Exception as col_e:
                    print(f"      ❌ FAILED to convert column '{col_name}'.")
                    print(f"         - Error: {col_e}")
                    raise col_e # Re-raise to stop the process

            # Previous attempts using vx.array(dict) and vx.struct(dict) failed.
            # New attempt: Use StructArray.from_fields, which seems more explicit.
            vortex_array = vx.StructArray.from_fields(vortex_columns)

            print(f"  ✅ Successfully converted Batch #{batch_num} to a Vortex Struct Array.")
            print(f"     - Vortex array type: {type(vortex_array)}")
            print(f"     - Vortex array length: {len(vortex_array)}")

        except Exception as e:
            print(f"  ❌ FAILED to convert Batch #{batch_num} to a Vortex Array.")
            print(f"     - Error Type: {type(e)}")
            print(f"     - Error Message: {e}")
            import traceback
            traceback.print_exc()
            # Stop after the first failure
            break
        
        print("-" * 50)

if __name__ == "__main__":
    debug_streaming_conversion()
