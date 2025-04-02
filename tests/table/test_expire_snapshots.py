import pytest
from pyiceberg.table import Table


def test_remove_snapshot(table_v2_with_extensive_snapshots: Table):
    table = table_v2_with_extensive_snapshots
    
    # Verify the table has metadata and a current snapshot before proceeding
    assert table.metadata is not None, "Table metadata is None"
    assert table.metadata.current_snapshot_id is not None, "Current snapshot ID is None"
    
    initial_snapshot_id = table.metadata.current_snapshot_id

    # Ensure the table has snapshots
    assert table.metadata.snapshots is not None, "Snapshots list is None"
    assert len(table.metadata.snapshots) == 2000, f"Expected 2000 snapshots, got {len(table.metadata.snapshots)}"

    # Print snapshot information for debugging
    print(f"Initial snapshot ID: {initial_snapshot_id}")
    print(f"Number of snapshots before expiry: {len(table.metadata.snapshots)}")
    
    # Remove a snapshot using the expire_snapshots API
    result = table.expire_snapshots().expire_snapshot_id(initial_snapshot_id).commit()
    
    # Ensure the commit operation returned a valid result
    assert result is not None, "Expire snapshots operation returned None"
    
    # Reload table to ensure we have the latest metadata
    table = table_v2_with_extensive_snapshots
    
    # Verify the snapshot is removed
    assert table.metadata.snapshots is not None, "Snapshots list is None after expiry"
    assert len(table.metadata.snapshots) == 1999, f"Expected 1999 snapshots after expiry, got {len(table.metadata.snapshots)}"
    assert all(snapshot["snapshot-id"] != initial_snapshot_id for snapshot in table.metadata.snapshots), "Snapshot not properly removed"
    
    # Check that the snapshot log was updated only if it exists
    if table.metadata.snapshot_log is not None:
        assert all(log["snapshot-id"] != initial_snapshot_id for log in table.metadata.snapshot_log), "Snapshot log not properly updated"

    # Test our helper function with a valid snapshot ID
    valid_snapshot_id = table.metadata.snapshots[0]["snapshot-id"]
    remove_snapshot(table.metadata, valid_snapshot_id)
    assert len(table.metadata.snapshots) == 1998, "Helper function failed to remove snapshot"
    
    # Ensure NoSuchSnapshotException is raised for non-existent snapshots
    with pytest.raises(ValueError, match="Snapshot with ID -1 does not exist"):
        remove_snapshot(table.metadata, -1)


