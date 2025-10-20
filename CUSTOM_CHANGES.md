# Custom Changes Tracking

This file tracks all custom modifications made to the `combined-prs-2434-2369-2627` branch.

## Branch Overview

- **Base**: `upstream/main` (apache/iceberg-python)
- **Contains**: PRs #2434, #2369, #2627 + custom enhancements
- **Remote**: `origin/combined-prs-2434-2369-2627` (ForeverAngry/iceberg-python)

---

## Custom Additions

### 1. Transaction Data File Handling Methods

**File**: `pyiceberg/table/__init__.py`

**Added Methods** (lines ~926-1028):

1. **`_is_valid_object_path(self, path: str) -> bool`**
   - Validates if a path is a valid object path (has URL scheme like s3://, gs://, etc.)
   - Used to distinguish between file paths and object storage paths

2. **`_does_file_exist(self, path: str) -> bool`**
   - Checks if a file exists using the table's IO implementation
   - Handles both local files and object storage paths
   - Returns True if file exists, False otherwise

3. **`get_data_files_from_objects(self, list_of_objects: List[str | pa.Table]) -> List[DataFile]`**
   - Converts a list of file paths or PyArrow tables to DataFile objects
   - Handles both Parquet files and in-memory tables
   - Validates file existence before conversion
   - Uses `_parquet_files_to_data_files` and `_dataframe_to_data_files` internally

4. **`add_data_files(self, data_files: List[DataFile], snapshot_properties: Dict[str, str] = EMPTY_DICT) -> None`**
   - Adds a list of DataFile objects to the transaction
   - Creates a FastAppendFiles operation
   - Applies snapshot properties (metadata)
   - Commits the operation to the transaction

**Tests**: `tests/table/test_transaction_data_files.py`

- 29 comprehensive tests covering all methods
- Tests for path validation, file existence, object conversion, and data file addition
- Integration tests for end-to-end workflows

---

## Configuration Changes

### 1. Pre-commit Configuration

**File**: `.pre-commit-config.yaml`

**Change**: Added exclusion pattern to skip linting on optional dependency files

```yaml
exclude: ^vendor/|^dev/rest-server/|^tests/catalog/test_rest_server.*\.py$
```

**Reason**: dev/rest-server files require FastAPI/uvicorn which are optional dependencies

---

## Conflict Resolutions

### 1. Thread Safety Test Conflicts

**File**: `tests/table/test_expire_snapshots.py`

**Issue**: PR #2434 changed `ExpireSnapshots._snapshot_ids_to_expire` from class attribute to instance attribute for thread safety

**Resolution**: Removed obsolete `.clear()` calls on class attribute (8 occurrences)

**Commits involved**:

- Merge of PR #2434 (thread safety fixes)
- Merge of PR #2369 (retention strategies)

---

## Merged Features

### PR #2434: Branch Merge Strategies

- **Tests**: `tests/table/test_branch_merge_strategies.py` (35 tests)
- **Core Changes**: `pyiceberg/table/update/snapshot.py`
- **Features**: Fast-forward, squash, rebase, cherry-pick, and merge strategies

### PR #2369: Snapshot Expiration with Retention Strategies

- **Tests**: `tests/table/test_expire_snapshots.py` (16 tests)
- **Core Changes**: `pyiceberg/table/update/snapshot.py`
- **Features**: Retention policies for snapshot expiration

### PR #2627: DynamoDB Catalog Enhancements

- **New Files**:
    - `dev/rest-server/` - Universal Iceberg REST Catalog Server
    - `tests/catalog/test_dynamodb_localstack.py`
    - `tests/catalog/test_rest_server*.py`
- **Modified**: `pyiceberg/catalog/dynamodb.py`, `tests/catalog/test_dynamodb.py`
- **Tests**: 57 tests
- **Features**: Enhanced DynamoDB catalog with REST server integration

### PR #2459: Vortex File Format Support

- **New Files**:
    - `pyiceberg/io/vortex.py` - Native Vortex file I/O module
    - `tests/io/test_vortex.py` - Vortex unit tests (377 lines)
    - `tests/integration/test_vortex_integration.py` - Vortex integration tests (576 lines)
- **Modified**:
    - `pyiceberg/io/pyarrow.py` - Added Vortex file format support with ORC handling
    - `pyiceberg/table/__init__.py` - Updated to use `_files_to_data_files` (supports Parquet, ORC, Vortex)
    - `pyiceberg/manifest.py` - Added Vortex to FileFormat enum
    - `tests/io/test_pyarrow.py` - Enhanced with Vortex tests
    - `tests/integration/test_add_files.py` - Added Vortex file integration
- **Features**:
    - Full support for Vortex columnar file format
    - Automatic format detection based on file extension (.parquet, .vortex)
    - Native PyArrow integration for Vortex files
    - Support for projection and filtering on Vortex files
    - Backwards compatible with existing Parquet workflows
- **Dependencies**: Added vortex-data package support (optional)

---

## Maintenance History

### Initial Creation (2024)

- Created `combined-prs-2434-2369-2627` from `upstream/main`
- Merged PRs #2434, #2369, #2627, #2459
- Resolved conflicts
- Fixed linting errors (B904 exception chaining)
- Added custom data file handling methods
- All tests passing (108 from first 3 PRs + 29 custom + Vortex tests)
- Updated custom methods to support Vortex file format via `_files_to_data_files`

### Future Updates

(Document each time you sync with upstream/main here)

---

## Important Notes

1. **Custom Code Location**: All custom additions are in `pyiceberg/table/__init__.py` (Transaction class)
2. **Test Coverage**: 100% test coverage for custom features
3. **Linting**: All 12 pre-commit hooks passing
4. **Compatibility**: Code uses modern Python 3.13+ syntax (`|` for type unions)
5. **File Format Support**: Automatically detects and handles Parquet, ORC, and Vortex file formats
6. **Vortex Integration**: Full Vortex support with optional vortex-data dependency

---

## Quick Reference

### Files with Custom Changes

- `pyiceberg/table/__init__.py` (4 new methods, updated to use `_files_to_data_files`)
- `tests/table/test_transaction_data_files.py` (29 tests, updated for Vortex compatibility)
- `.pre-commit-config.yaml` (exclusion pattern)

### Total Changes vs Upstream

- **Commits ahead**: 24+ (including Vortex merge)
- **Files changed**: 20+ (10+ added, 14+ modified)
- **Lines added**: ~3000+
- **Tests**: All passing (includes Vortex integration tests)
- **File Formats Supported**: Parquet, ORC, Vortex

---

## Recovery Information

### Backup Branches

List backup branches created during maintenance:

- (none yet - use `maintain-branch.sh` to create backups automatically)

### Git Tags

Important milestone tags:

- (none yet - consider tagging: `git tag -a v1.0-custom-combined -m "Initial combined branch with data file methods"`)

---

Last Updated: 2024
