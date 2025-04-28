#!/usr/bin/env python3
'''
By Gemini
'''
import os
import re
import argparse
from datetime import datetime, timedelta, time

# --- Configuration ---

# Directory containing the backup files
current_dir = os.path.dirname(os.path.realpath(__file__))
BACKUP_DIR = current_dir # <<<--- IMPORTANT: SET THIS PATH

# Regex pattern to find backup files and capture the timestamp
# Assumes YYYYMMDD_HHMMSS format. Adjust if your format differs.
# Example: db_20250428_102932.sqlite3.xz
FILENAME_PATTERN = r'^db_(\d{8})_(\d{6})\.(.+)\.xz$'
TIMESTAMP_FORMAT = '%Y%m%d_%H%M%S'

# --- Retention Schedule ---
# Define how many backups to keep for each period.
# The script keeps the *newest* N backups for each distinct period (hour, day, week, etc.).
# For example, keep_daily=7 means keeping the newest backup for each of the 7 most recent days
# that have backups. It does NOT mean keeping 7 backups from the last 24 hours * 7 days.
# Set a value to 0 or None to disable keeping backups for that period type.

KEEP_HOURLY  = 3   # Keep the last N hourly backups
KEEP_DAILY   = 7    # Keep the last N daily backups
KEEP_WEEKLY  = 4    # Keep the last N weekly backups
KEEP_MONTHLY = 12   # Keep the last N monthly backups
KEEP_QUARTERLY = 8  # Keep the last N quarterly backups (4 per year * 2 years)
KEEP_HALFYEARLY = 4 # Keep the last N half-yearly backups (2 per year * 2 years)
KEEP_YEARLY  = 10   # Keep the last N yearly backups

# --- Helper Functions ---

def get_period_start(dt, period):
    """Calculates the start datetime for a given period containing dt."""
    if period == 'hourly':
        return dt.replace(minute=0, second=0, microsecond=0)
    elif period == 'daily':
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == 'weekly':
        # Start of the week (Monday)
        start_of_day = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        return start_of_day - timedelta(days=start_of_day.weekday())
    elif period == 'monthly':
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif period == 'quarterly':
        start_of_month = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        quarter_month = ((start_of_month.month - 1) // 3) * 3 + 1
        return start_of_month.replace(month=quarter_month)
    elif period == 'halfyearly':
        start_of_month = dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        halfyear_month = 1 if start_of_month.month <= 6 else 7
        return start_of_month.replace(month=halfyear_month)
    elif period == 'yearly':
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        raise ValueError(f"Unknown period: {period}")

# --- Main Logic ---

def parse_backup_files(backup_dir, pattern, time_format):
    """Scans the directory, parses filenames, and returns a sorted list of (path, datetime)"""
    backups = []
    filename_re = re.compile(pattern)
    print(f"Scanning directory: {backup_dir}")
    try:
        for filename in os.listdir(backup_dir):
            match = filename_re.match(filename)
            if match:
                timestamp_str = f"{match.group(1)}_{match.group(2)}"
                try:
                    backup_time = datetime.strptime(timestamp_str, time_format)
                    full_path = os.path.join(backup_dir, filename)
                    backups.append({'path': full_path, 'time': backup_time})
                except ValueError:
                    print(f"Warning: Could not parse timestamp in filename: {filename}")
            # else:
            #     print(f"Info: Skipping file not matching pattern: {filename}")
    except FileNotFoundError:
        print(f"Error: Backup directory not found: {backup_dir}")
        return None
    except Exception as e:
        print(f"Error scanning directory {backup_dir}: {e}")
        return None

    # Sort backups by time, newest first
    backups.sort(key=lambda x: x['time'], reverse=True)
    print(f"Found {len(backups)} backup files matching the pattern.")
    return backups

def apply_retention_policy(backups, schedule):
    """Applies the retention policy and returns a set of backup dicts to keep."""
    if not backups:
        return set()

    to_keep = set()
    kept_markers = {
        'hourly': set(), 'daily': set(), 'weekly': set(),
        'monthly': set(), 'quarterly': set(), 'halfyearly': set(), 'yearly': set()
    }
    period_limits = {
        'hourly': schedule.get('hourly', 0) or 0,
        'daily': schedule.get('daily', 0) or 0,
        'weekly': schedule.get('weekly', 0) or 0,
        'monthly': schedule.get('monthly', 0) or 0,
        'quarterly': schedule.get('quarterly', 0) or 0,
        'halfyearly': schedule.get('halfyearly', 0) or 0,
        'yearly': schedule.get('yearly', 0) or 0,
    }

    # Always keep the newest backup
    newest_backup = backups[0]
    newest_backup_tuple = tuple(newest_backup.items()) # Use tuple for set compatibility
    to_keep.add(newest_backup_tuple)
    print(f"Always keeping the newest backup: {os.path.basename(newest_backup['path'])} ({newest_backup['time']})")

    # Update markers based on the newest backup
    for period in kept_markers:
        if period_limits[period] > 0:
            period_start = get_period_start(newest_backup['time'], period)
            kept_markers[period].add(period_start)

    # Iterate through the rest of the backups (newest to oldest)
    for backup in backups[1:]:
        backup_time = backup['time']
        # keep_this_one = False
        backup_tuple = tuple(backup.items()) # Use tuple for set compatibility

        # Check against each period rule
        for period, limit in period_limits.items():
            if limit > 0:
                period_start = get_period_start(backup_time, period)
                # Check if we still need backups for this period type
                # AND if this backup falls into a period slot we haven't kept yet
                if len(kept_markers[period]) < limit and period_start not in kept_markers[period]:
                    to_keep.add(backup_tuple)
                    kept_markers[period].add(period_start)
                    # keep_this_one = True
                    # print(f"  - Keeping {os.path.basename(backup['path'])} for {period} rule ({len(kept_markers[period])}/{limit})")
                    # No need to check shorter periods if a longer one keeps it
                    # break # Removed break: A backup might satisfy multiple rules, let it fill slots if needed

    print(f"\nTotal backups to keep based on schedule: {len(to_keep)}")
    # Convert back from tuples to dicts for easier use later if needed
    kept_dicts = [dict(item) for item in to_keep]
    return kept_dicts

def create_arg_parser():
    '''
    Create argument parser for terminal parameters.
    '''
    parser = argparse.ArgumentParser(description=("Prune backup files based on a "
                                                  "retention schedule."))
    parser.add_argument("backup_dir", nargs='?', default=BACKUP_DIR,
                        help=f"Directory containing backup files (default: {BACKUP_DIR})")
    parser.add_argument("--pattern", default=FILENAME_PATTERN,
                        help=f"Regex pattern for filenames (default: {FILENAME_PATTERN})")
    parser.add_argument("--time-format", default=TIMESTAMP_FORMAT,
                        help=f"Timestamp format string for parsing (default: {TIMESTAMP_FORMAT})")
    parser.add_argument("--hourly", type=int, default=KEEP_HOURLY, 
                        help="Number of hourly backups to keep")
    parser.add_argument("--daily", type=int, default=KEEP_DAILY, 
                        help="Number of daily backups to keep")
    parser.add_argument("--weekly", type=int, default=KEEP_WEEKLY, 
                        help="Number of weekly backups to keep")
    parser.add_argument("--monthly", type=int, default=KEEP_MONTHLY, 
                        help="Number of monthly backups to keep")
    parser.add_argument("--quarterly", type=int, default=KEEP_QUARTERLY, 
                        help="Number of quarterly backups to keep")
    parser.add_argument("--halfyearly", type=int, default=KEEP_HALFYEARLY, 
                        help="Number of half-yearly backups to keep")
    parser.add_argument("--yearly", type=int, default=KEEP_YEARLY, 
                        help="Number of yearly backups to keep")
    parser.add_argument("--execute", action="store_true",
                        help="Actually delete the files. Default is dry run.")

    args = parser.parse_args()
    return args

def main():
    '''
    Entry point.
    '''
    args = create_arg_parser()

    schedule = {
        'hourly': args.hourly,
        'daily': args.daily,
        'weekly': args.weekly,
        'monthly': args.monthly,
        'quarterly': args.quarterly,
        'halfyearly': args.halfyearly,
        'yearly': args.yearly,
    }

    # Get all matching backup files, sorted newest first
    all_backups = parse_backup_files(args.backup_dir, args.pattern, args.time_format)

    if all_backups is None:
        return # Error occurred during scanning

    if not all_backups:
        print("No backup files found matching the pattern. Nothing to do.")
        return

    # Determine which backups to keep based on the policy
    backups_to_keep = apply_retention_policy(all_backups, schedule)
    paths_to_keep = {b['path'] for b in backups_to_keep}
    all_backup_paths = {b['path'] for b in all_backups}

    # Determine which backups to prune (those not in the keep set)
    paths_to_prune = sorted(list(all_backup_paths - paths_to_keep))

    print("\n--- Pruning Plan ---")
    print(f"Total backups found: {len(all_backup_paths)}")
    print(f"Backups to keep:   {len(paths_to_keep)}")
    print(f"Backups to prune:  {len(paths_to_prune)}")

    if not paths_to_prune:
        print("\nNo backups need to be pruned.")
    else:
        print("\nFiles to be pruned:")
        for file_path in paths_to_prune:
            print(f"  - {os.path.basename(file_path)}")

        if args.execute:
            print("\n--- EXECUTING DELETION ---")
            deleted_count = 0
            error_count = 0
            for file_path in paths_to_prune:
                try:
                    os.remove(file_path)
                    print(f"  Deleted: {os.path.basename(file_path)}")
                    deleted_count += 1
                except OSError as e:
                    print(f"  Error deleting {os.path.basename(file_path)}: {e}")
                    error_count += 1
            print(f"\nDeletion complete. Deleted: {deleted_count}, Errors: {error_count}")
        else:
            print("\n--- DRY RUN ---")
            print("No files were deleted. Use the --execute flag to perform deletion.")

if __name__ == "__main__":
    main()
