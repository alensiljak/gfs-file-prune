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
        'db_20150428_180000.sqlite3.xz', # 10 years <=
        'db_20160428_180000.sqlite3.xz',
        'db_20170428_180000.sqlite3.xz',
        'db_20180428_180000.sqlite3.xz',
        'db_20190428_180000.sqlite3.xz', # <=
        'db_20191028_180000.sqlite3.xz',
        'db_20200428_180000.sqlite3.xz', # <=
        'db_20201028_180000.sqlite3.xz',
        'db_20210428_180000.sqlite3.xz',
        'db_20211028_180000.sqlite3.xz',
        'db_20220428_180000.sqlite3.xz',
        'db_20221028_180000.sqlite3.xz', # 2.5 years
        'db_20230428_180000.sqlite3.xz', # 2 years
        'db_20231028_180000.sqlite3.xz', # 1.5 years
        'db_20240428_180000.sqlite3.xz', # 1 year
        'db_20241028_180000.sqlite3.xz', # 6 months
        'db_20240128_180000.sqlite3.xz', # 3 months
        'db_20250328_180000.sqlite3.xz',
        'db_20250428_180000.sqlite3.xz'
    ]
    # reverse the order.
    filenames.sort(reverse=True)

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
    policy = prune.Policy()
    backups_to_keep = policy.apply_retention_policy(all_backups, schedule)

    all_times = {b['time'].strftime(ISO_FORMAT) for b in all_backups}
    times_to_keep = {b['time'].strftime(ISO_FORMAT) for b in backups_to_keep}
    times_to_prune = all_times - times_to_keep

    assert backups_to_keep is not None
    assert len(backups_to_keep) > 0

    expected_to_remove = [
        '2015-04-28 18:00:00',
        '2019-04-28 18:00:00',
        '2020-04-28 18:00:00'
    ]

    print(f'to prune: {sorted(times_to_prune)}')
    print(f'to keep: {sorted(times_to_keep)}')

    # assert times_to_keep == set(expected_to_keep)
    assert times_to_prune == set(expected_to_remove)

def real_case_test_1():
    '''
        Debug/test a real case scenario.
    '''
    filenames = [
        'db_20250428_102932.sqlite3.xz',
        'db_20250428_160002.sqlite3.xz',
        'db_20250428_215619.sqlite3.xz',
        'db_20250429_100002.sqlite3.xz',
        'db_20250429_160003.sqlite3.xz'
    ]
    filenames.sort(reverse=True)
