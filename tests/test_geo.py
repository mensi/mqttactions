import pytest
from unittest.mock import patch, Mock
from datetime import datetime, timedelta, time, date
from zoneinfo import ZoneInfo
from contextlib import contextmanager

from mqttactions.geo import Location

# A known location: Berlin, Germany
TEST_LAT = 52.52
TEST_LON = 13.40
TEST_TZ = 'Europe/Berlin'
BERLIN_TZ = ZoneInfo(TEST_TZ)

# Use known historical data for Berlin on Jan 1st/2nd, 2000
DATE_2000_01_01 = date(2000, 1, 1)
DATE_2000_01_02 = date(2000, 1, 2)

# Expected local times for events in Berlin based on user-provided values.
# We will check if suntime is within a tolerance of these.
EXPECTED_SUNRISE_2000_01_01 = datetime(2000, 1, 1, 8, 17, tzinfo=BERLIN_TZ)
EXPECTED_SUNSET_2000_01_01 = datetime(2000, 1, 1, 16, 2, tzinfo=BERLIN_TZ)
EXPECTED_SUNRISE_2000_01_02 = datetime(2000, 1, 2, 8, 17, tzinfo=BERLIN_TZ)
EXPECTED_SUNSET_2000_01_02 = datetime(2000, 1, 2, 16, 3, tzinfo=BERLIN_TZ)


@contextmanager
def patch_datetime_now(dt_to_return):
    """Context manager to patch datetime.now() for the duration of a test."""
    with patch('mqttactions.geo.datetime') as mock_dt:
        mock_dt.now.return_value = dt_to_return
        mock_dt.combine.side_effect = datetime.combine
        yield mock_dt


@pytest.fixture
def location():
    with patch('mqttactions.geo.Location._start_scheduler'):
        loc = Location(lat=TEST_LAT, lon=TEST_LON)
        yield loc


def test_on_sunrise_before_sunrise(location):
    mock_now = EXPECTED_SUNRISE_2000_01_01 - timedelta(hours=1)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_sunrise()(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert abs(scheduled_time - EXPECTED_SUNRISE_2000_01_01) < timedelta(minutes=5)


def test_on_sunrise_after_sunrise(location):
    mock_now = EXPECTED_SUNRISE_2000_01_01 + timedelta(hours=1)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_sunrise()(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert abs(scheduled_time - EXPECTED_SUNRISE_2000_01_02) < timedelta(minutes=5)


def test_on_sunset_before_sunset(location):
    mock_now = EXPECTED_SUNSET_2000_01_01 - timedelta(hours=1)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_sunset()(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert abs(scheduled_time - EXPECTED_SUNSET_2000_01_01) < timedelta(minutes=5)


def test_on_sunset_after_sunset(location):
    mock_now = EXPECTED_SUNSET_2000_01_01 + timedelta(hours=1)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_sunset()(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert abs(scheduled_time - EXPECTED_SUNSET_2000_01_02) < timedelta(minutes=5)


def test_on_localtime_before_target(location):
    target_time = time(10, 0)
    mock_now = datetime(2000, 1, 1, 8, 0, tzinfo=BERLIN_TZ)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_localtime(target_time)(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert scheduled_time.date() == DATE_2000_01_01
        assert scheduled_time.time() == target_time


def test_on_localtime_after_target(location):
    target_time = time(10, 0)
    mock_now = datetime(2000, 1, 1, 12, 0, tzinfo=BERLIN_TZ)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_localtime(target_time)(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert scheduled_time.date() == DATE_2000_01_02
        assert scheduled_time.time() == target_time


def test_on_sunrise_with_offset(location):
    offset = timedelta(minutes=-15)
    mock_now = EXPECTED_SUNRISE_2000_01_01 - timedelta(hours=1)
    with patch_datetime_now(mock_now):
        callback_func = Mock()
        location.on_sunrise(offset=offset)(callback_func)

        assert len(location._pending_callbacks) == 1
        scheduled_time = location._pending_callbacks[0].datetime
        assert abs(scheduled_time - (EXPECTED_SUNRISE_2000_01_01 + offset)) < timedelta(minutes=5)


def test_callback_reschedules_itself(location):
    mock_now = EXPECTED_SUNRISE_2000_01_01 - timedelta(hours=1)
    with patch_datetime_now(mock_now) as mock_datetime:
        callback_func = Mock()
        location.on_sunrise()(callback_func)

        assert len(location._pending_callbacks) == 1
        job = location._pending_callbacks[0].callable

        location._pending_callbacks.clear()

        # When the job runs, the time will have advanced.
        # Set "now" to the time of the event to simulate this.
        mock_datetime.now.return_value = EXPECTED_SUNRISE_2000_01_01
        job()

        callback_func.assert_called_once()
        assert len(location._pending_callbacks) == 1

        scheduled_time = location._pending_callbacks[0].datetime
        assert abs(scheduled_time - EXPECTED_SUNRISE_2000_01_02) < timedelta(minutes=5)
