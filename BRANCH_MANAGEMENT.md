# Branch Management Strategy for combined-prs-2434-2369-2627

## Current Branch Contents

Your branch `combined-prs-2434-2369-2627` contains:

1. **PR #2434** - Branch merge strategies
2. **PR #2369** - Snapshot expiration with retention strategies
3. **PR #2627** - DynamoDB catalog enhancements
4. **Custom additions** - Transaction data file handling methods

## Files Modified/Added

### Added Files

- `dev/docker-compose-rest-server.yml`
- `dev/rest-server/.pyiceberg.yaml`
- `dev/rest-server/Dockerfile`
- `dev/rest-server/main.py`
- `dev/run-rest-server.sh`
- `tests/catalog/test_dynamodb_localstack.py`
- `tests/catalog/test_rest_server.py`
- `tests/catalog/test_rest_server_dynamodb.py`
- `tests/catalog/test_rest_server_with_dynamodb_integration.py`
- `tests/table/test_branch_merge_strategies.py`
- `tests/table/test_transaction_data_files.py`

### Modified Files

- `.pre-commit-config.yaml` (excluded dev/rest-server from linting)
- `mkdocs/docs/api.md`
- `pyiceberg/catalog/dynamodb.py`
- `pyiceberg/table/__init__.py` (added data file methods)
- `pyiceberg/table/metadata.py`
- `pyiceberg/table/update/snapshot.py`
- `tests/catalog/test_dynamodb.py`
- `tests/conftest.py`
- `tests/table/test_expire_snapshots.py`

---

## Strategy 1: Regular Rebase (RECOMMENDED)

Keep your branch up-to-date by regularly rebasing on main.

### When to use

- You want a clean, linear history
- You're working on a feature branch that will eventually be merged
- You want to catch conflicts early

### How to rebase

```bash
# 1. Fetch latest changes from upstream
git fetch upstream

# 2. Rebase your branch on top of upstream/main
git rebase upstream/main

# 3. If conflicts occur, resolve them:
#    - Edit conflicting files
#    - git add <resolved-files>
#    - git rebase --continue

# 4. Force push to your remote (since history was rewritten)
git push origin combined-prs-2434-2369-2627 --force-with-lease
```

### Strategy 1 Advantages

✅ Clean, linear history
✅ Easy to see what changed
✅ Conflicts resolved incrementally

### Strategy 1 Disadvantages

⚠️ Rewrites history (need force push)
⚠️ Can be complex if many conflicts

---

## Strategy 2: Merge from Main

Periodically merge main into your branch.

### When to merge

- You want to preserve exact commit history
- Multiple people are working on the same branch
- You don't want to rewrite history

### How to merge

```bash
# 1. Fetch latest changes
git fetch upstream

# 2. Merge upstream/main into your branch
git merge upstream/main

# 3. If conflicts occur, resolve them:
#    - Edit conflicting files
#    - git add <resolved-files>
#    - git commit

# 4. Push to your remote
git push origin combined-prs-2434-2369-2627
```

### Strategy 2 Advantages

✅ Preserves history
✅ No force push needed
✅ Easier for collaboration

### Strategy 2 Disadvantages

⚠️ Creates merge commits
⚠️ History can be harder to read

---

## Strategy 3: Create a Backup Branch (SAFEST)

Always create a backup before major operations.

### How to create backups

```bash
# Create a backup of your current branch
git branch combined-prs-2434-2369-2627-backup

# Push the backup to remote
git push origin combined-prs-2434-2369-2627-backup

# Now you can safely rebase or merge
git rebase upstream/main
```

If something goes wrong:

```bash
# Reset to your backup
git reset --hard combined-prs-2434-2369-2627-backup
```

---

## Strategy 4: Track Changes with Git Patches

Create patches of your custom changes for easy reapplication.

### How to create patches

```bash
# Create patches for your custom commits
git format-patch upstream/main..HEAD -o ~/iceberg-patches/

# This creates individual patch files for each commit
# You can reapply them later with:
git am ~/iceberg-patches/*.patch
```

---

## Recommended Workflow

### Weekly Maintenance

```bash
# 1. Create a backup
git branch combined-prs-backup-$(date +%Y%m%d)
git push origin combined-prs-backup-$(date +%Y%m%d)

# 2. Fetch updates
git fetch upstream

# 3. Check what changed in main
git log --oneline HEAD..upstream/main

# 4. Rebase on main (or merge if you prefer)
git rebase upstream/main

# 5. Run tests
make lint
python -m pytest tests/table/test_branch_merge_strategies.py \
                 tests/table/test_expire_snapshots.py \
                 tests/catalog/test_dynamodb.py \
                 tests/table/test_transaction_data_files.py -v

# 6. Push (with force-with-lease for rebase)
git push origin combined-prs-2434-2369-2627 --force-with-lease
```

---

## Handling Conflicts

When you encounter conflicts during rebase or merge:

### Step 1: Identify conflicting files

```bash
git status
```

### Step 2: Open conflicting files and look for markers

```text
<<<<<<< HEAD (your changes)
your code
=======
their code from main
>>>>>>> upstream/main
```

### Step 3: Resolve conflicts

- Keep your changes, their changes, or both
- Remove the conflict markers
- Test the code

### Step 4: Mark as resolved

```bash
git add <resolved-file>
```

### Step 5: Continue

```bash
# For rebase:
git rebase --continue

# For merge:
git commit
```

---

## Tracking Your Custom Changes

### Create a changelog of your additions

```bash
# List your custom commits
git log upstream/main..HEAD --oneline > CUSTOM_CHANGES.txt

# Or create a detailed summary
git log upstream/main..HEAD --stat > DETAILED_CHANGES.txt
```

### Tag important states

```bash
# Tag your current state
git tag -a v1.0-custom-combined -m "Combined PRs with data file methods"
git push origin v1.0-custom-combined
```

---

## Emergency Recovery

If something goes wrong:

### Step 1: Check reflog to find previous state

```bash
git reflog
```

### Step 2: Reset to a previous state

```bash
git reset --hard HEAD@{5}  # or whatever reflog entry
```

### Step 3: Or reset to remote backup

```bash
git reset --hard origin/combined-prs-2434-2369-2627-backup
```

---

## Summary

**For your use case, I recommend:**

1. **Create a backup branch** before any major operations
2. **Use rebase** to keep history clean (weekly or before major changes)
3. **Run tests** after every rebase/merge
4. **Tag stable states** so you can easily return to them
5. **Document your custom changes** in this file

This ensures you'll never lose your work while keeping your branch up-to-date with main! 🎉
