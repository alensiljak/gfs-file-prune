'''
    Test the retention policy
'''
from datetime import datetime
import prune


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

def test_dev():
    '''
    dev test
    '''
    filenames = [
        'db_20250428_180000.sqlite3.xz'
    ]
    #prune.main()
    