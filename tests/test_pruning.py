'''
    Test the retention policy
'''
from datetime import datetime
import prune

ISO_FORMAT = '%Y-%m-%d %H:%M:%S'

def test_parsing_filenames():
    ''' Test parsing of filenames '''
    filenames = [
        'db_20250428_180000.sqlite3.xz'
    ]

    result = prune.parse_filenames(filenames, '', prune.FILENAME_PATTERN,
                                   prune.TIMESTAMP_FORMAT)

    expected = [
        {'path': filenames[0],
         'time': datetime.strptime('20250428_180000', prune.TIMESTAMP_FORMAT)}
    ]

    assert result is not None
    assert len(result) > 0
    assert result == expected

def test_retention():
    ''' dev test '''
    filenames = [
        'db_20250428_180000.sqlite3.xz'
    ]

    #prune.main()
    schedule = {
        "hourly": prune.KEEP_HOURLY,
        "daily": prune.KEEP_DAILY,
        "weekly": prune.KEEP_WEEKLY,
        "monthly": prune.KEEP_MONTHLY,
        "quarterly": prune.KEEP_QUARTERLY,
        "halfyearly": prune.KEEP_HALFYEARLY,
        "yearly": prune.KEEP_YEARLY,
    }
    all_backups = prune.parse_filenames(filenames, '', prune.FILENAME_PATTERN,
                                        prune.TIMESTAMP_FORMAT)
    backups_to_keep = prune.apply_retention_policy(all_backups, schedule)

    all_times = {b['time'].strftime(ISO_FORMAT) for b in all_backups}
    times_to_keep = {b['time'].strftime(ISO_FORMAT) for b in backups_to_keep}
    times_to_prune = sorted(list(all_times - times_to_keep))

    assert backups_to_keep is not None
    assert len(backups_to_keep) > 0

    expected = [
        '2025-04-28 18:00:00'
    ]

    print(f'to prune: {times_to_prune}')
    print(f'to keep: {times_to_keep}')

    assert times_to_keep == set(expected)
