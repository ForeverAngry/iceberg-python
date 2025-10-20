#!/bin/bash

# Branch Maintenance Script for combined-prs-2434-2369-2627
# This script helps keep your branch up-to-date with upstream/main

set -e  # Exit on error

BRANCH_NAME="combined-prs-2434-2369-2627"
UPSTREAM="upstream"
REMOTE="origin"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================"
echo "Branch Maintenance Script"
echo "================================================"
echo ""

# Check current branch
current_branch=$(git rev-parse --abbrev-ref HEAD)
if [ "$current_branch" != "$BRANCH_NAME" ]; then
    echo -e "${RED}Error: You're on branch '$current_branch', not '$BRANCH_NAME'${NC}"
    echo "Switch to $BRANCH_NAME first with: git checkout $BRANCH_NAME"
    exit 1
fi

echo -e "${GREEN}✓ On correct branch: $BRANCH_NAME${NC}"
echo ""

# Step 1: Create backup
backup_name="${BRANCH_NAME}-backup-$(date +%Y%m%d-%H%M%S)"
echo "Step 1: Creating backup branch..."
git branch "$backup_name"
echo -e "${GREEN}✓ Created backup: $backup_name${NC}"
echo ""

# Step 2: Fetch upstream
echo "Step 2: Fetching updates from upstream..."
git fetch "$UPSTREAM"
echo -e "${GREEN}✓ Fetched from $UPSTREAM${NC}"
echo ""

# Step 3: Show what's new
echo "Step 3: Checking for new changes in upstream/main..."
new_commits=$(git log --oneline HEAD.."$UPSTREAM/main" | wc -l | tr -d ' ')
if [ "$new_commits" -eq 0 ]; then
    echo -e "${GREEN}✓ No new changes in upstream/main. Your branch is up-to-date!${NC}"
    echo ""
    echo "Cleaning up backup branch..."
    git branch -D "$backup_name"
    echo "Done!"
    exit 0
fi

echo -e "${YELLOW}Found $new_commits new commits in upstream/main:${NC}"
git log --oneline HEAD.."$UPSTREAM/main" --max-count=10
echo ""

# Step 4: Ask user which strategy to use
echo "Choose merge strategy:"
echo "  1) Rebase (recommended - cleaner history, requires force push)"
echo "  2) Merge (preserves history, no force push needed)"
echo "  3) Cancel (keep backup, don't update)"
echo ""
read -p "Enter choice [1-3]: " choice

case $choice in
    1)
        echo ""
        echo "Step 4: Rebasing on upstream/main..."
        if git rebase "$UPSTREAM/main"; then
            echo -e "${GREEN}✓ Rebase successful!${NC}"
            echo ""
            echo "Step 5: Running tests..."
            if make lint && python -m pytest tests/table/test_branch_merge_strategies.py \
                             tests/table/test_expire_snapshots.py \
                             tests/catalog/test_dynamodb.py \
                             tests/table/test_transaction_data_files.py -v; then
                echo -e "${GREEN}✓ All tests passed!${NC}"
                echo ""
                echo "Step 6: Pushing to remote (force with lease)..."
                git push "$REMOTE" "$BRANCH_NAME" --force-with-lease
                echo -e "${GREEN}✓ Successfully pushed to $REMOTE/$BRANCH_NAME${NC}"
                echo ""
                echo "Cleaning up backup branch..."
                git push "$REMOTE" "$backup_name"
                echo -e "${GREEN}✓ Backup also pushed to remote: $backup_name${NC}"
                echo ""
                echo -e "${GREEN}Success! Your branch is now up-to-date.${NC}"
            else
                echo -e "${RED}✗ Tests failed! Please fix the issues.${NC}"
                echo "Your backup is at: $backup_name"
                echo "To restore: git reset --hard $backup_name"
                exit 1
            fi
        else
            echo -e "${RED}✗ Rebase failed! There are conflicts to resolve.${NC}"
            echo ""
            echo "To resolve conflicts:"
            echo "  1. Edit the conflicting files"
            echo "  2. git add <resolved-files>"
            echo "  3. git rebase --continue"
            echo ""
            echo "To abort the rebase:"
            echo "  git rebase --abort"
            echo "  git reset --hard $backup_name"
            echo ""
            echo "Your backup is at: $backup_name"
            exit 1
        fi
        ;;
    2)
        echo ""
        echo "Step 4: Merging upstream/main..."
        if git merge "$UPSTREAM/main"; then
            echo -e "${GREEN}✓ Merge successful!${NC}"
            echo ""
            echo "Step 5: Running tests..."
            if make lint && python -m pytest tests/table/test_branch_merge_strategies.py \
                             tests/table/test_expire_snapshots.py \
                             tests/catalog/test_dynamodb.py \
                             tests/table/test_transaction_data_files.py -v; then
                echo -e "${GREEN}✓ All tests passed!${NC}"
                echo ""
                echo "Step 6: Pushing to remote..."
                git push "$REMOTE" "$BRANCH_NAME"
                echo -e "${GREEN}✓ Successfully pushed to $REMOTE/$BRANCH_NAME${NC}"
                echo ""
                echo "Cleaning up backup branch..."
                git push "$REMOTE" "$backup_name"
                echo -e "${GREEN}✓ Backup also pushed to remote: $backup_name${NC}"
                echo ""
                echo -e "${GREEN}Success! Your branch is now up-to-date.${NC}"
            else
                echo -e "${RED}✗ Tests failed! Please fix the issues.${NC}"
                echo "Your backup is at: $backup_name"
                echo "To restore: git reset --hard $backup_name"
                exit 1
            fi
        else
            echo -e "${RED}✗ Merge failed! There are conflicts to resolve.${NC}"
            echo ""
            echo "To resolve conflicts:"
            echo "  1. Edit the conflicting files"
            echo "  2. git add <resolved-files>"
            echo "  3. git commit"
            echo ""
            echo "To abort the merge:"
            echo "  git merge --abort"
            echo "  git reset --hard $backup_name"
            echo ""
            echo "Your backup is at: $backup_name"
            exit 1
        fi
        ;;
    3)
        echo ""
        echo "Cancelled. Your backup is at: $backup_name"
        echo "You can push it to remote with:"
        echo "  git push $REMOTE $backup_name"
        exit 0
        ;;
    *)
        echo -e "${RED}Invalid choice. Cancelled.${NC}"
        echo "Your backup is at: $backup_name"
        exit 1
        ;;
esac
