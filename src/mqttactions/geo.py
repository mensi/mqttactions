import threading
import time as time_module
from collections import namedtuple
from datetime import datetime, timedelta, time
from typing import Callable, Optional
from timezonefinder import TimezoneFinder
from suntime import Sun
from zoneinfo import ZoneInfo
import logging

logger = logging.getLogger(__name__)

Callback = namedtuple("Callback", ["datetime", "callable"])


class Location:
    """Represents a geographical location with astronomical event scheduling capabilities."""

    def __init__(self, lat: float, lon: float):
        """Initialize a geographical location.

        Args:
            lat: Latitude in decimal degrees
            lon: Longitude in decimal degrees
        """
        self.lat = lat
        self.lon = lon
        self._sun = Sun(lat, lon)

        # Get the timezone for this location
        timezone_str = TimezoneFinder().timezone_at(lat=lat, lng=lon)
        if timezone_str is None:
            raise ValueError(f"Could not determine timezone for coordinates ({lat}, {lon})")

        self._timezone = ZoneInfo(timezone_str)
        self._scheduler_thread = None
        self._running = False
        self._pending_callbacks: list[Callback] = []

        # Start the scheduler thread
        self._start_scheduler()

    def _get_sunrise_time(self, dt: datetime) -> datetime:
        """Get the sunrise time and hack around https://github.com/SatAgro/suntime/issues/30."""
        td = self._sun.get_sun_timedelta(dt, self._timezone, is_rise_time=True)
        return datetime.combine(dt.date(), time(tzinfo=self._timezone)) + timedelta(seconds=td.seconds)

    def _get_sunset_time(self, dt: datetime) -> datetime:
        """Get the sunset time and hack around https://github.com/SatAgro/suntime/issues/30."""
        td = self._sun.get_sun_timedelta(dt, self._timezone, is_rise_time=False)
        return datetime.combine(dt.date(), time(tzinfo=self._timezone)) + timedelta(seconds=td.seconds)

    def _start_scheduler(self):
        """Start the background scheduler thread."""
        if self._scheduler_thread is None or not self._scheduler_thread.is_alive():
            self._running = True
            self._scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
            self._scheduler_thread.start()

    def _run_scheduler(self):
        """Run the scheduler in a background thread."""
        while self._running:
            now = datetime.now(tz=self._timezone)
            for cb in self._pending_callbacks[:]:
                try:
                    if now >= cb.datetime:
                        self._pending_callbacks.remove(cb)
                        cb.callable()
                except Exception as e:
                    print(f"Error executing scheduled callback: {e}")
            time_module.sleep(1)

    def on_sunrise(self, offset: Optional[timedelta] = None) -> Callable:
        """Decorator to schedule a function to run at sunrise every day.

        Args:
            offset: Optional offset from sunrise time

        Returns:
            The decorator function
        """

        def decorator(func: Callable) -> Callable:
            effective_offset = offset or timedelta()

            def job():
                func()
                # Schedule for next day
                tomorrow_date = datetime.now(self._timezone).date() + timedelta(days=1)
                noon_tomorrow = datetime.combine(tomorrow_date, time(12, 0))
                next_sunrise = self._get_sunrise_time(noon_tomorrow)
                self._pending_callbacks.append(Callback(next_sunrise + effective_offset, job))

            # Schedule first event
            now = datetime.now(self._timezone)
            noon_today = datetime.combine(now.date(), time(12, 0))
            next_event_time = self._get_sunrise_time(noon_today) + effective_offset

            if next_event_time <= now:
                noon_tomorrow = datetime.combine(now.date() + timedelta(days=1), time(12, 0))
                next_event_time = (
                    self._get_sunrise_time(noon_tomorrow) + effective_offset
                )

            self._pending_callbacks.append(Callback(next_event_time, job))
            return func

        return decorator

    def on_sunset(self, offset: Optional[timedelta] = None) -> Callable:
        """Decorator to schedule a function to run at sunset every day.

        Args:
            offset: Optional offset from sunset time

        Returns:
            The decorator function
        """

        def decorator(func: Callable) -> Callable:
            effective_offset = offset or timedelta()

            def job():
                func()
                # Schedule for next day
                tomorrow_date = datetime.now(self._timezone).date() + timedelta(days=1)
                noon_tomorrow = datetime.combine(tomorrow_date, time(12, 0))
                next_sunset = self._get_sunset_time(noon_tomorrow)
                self._pending_callbacks.append(Callback(next_sunset + effective_offset, job))

            # Schedule first event
            now = datetime.now(self._timezone)
            noon_today = datetime.combine(now.date(), time(12, 0))
            next_event_time = self._get_sunset_time(noon_today) + effective_offset

            if next_event_time <= now:
                noon_tomorrow = datetime.combine(now.date() + timedelta(days=1), time(12, 0))
                next_event_time = (
                    self._get_sunset_time(noon_tomorrow) + effective_offset
                )

            self._pending_callbacks.append(Callback(next_event_time, job))
            return func

        return decorator

    def localtime(self):
        """Get the current local time."""
        return datetime.now(self._timezone).time()

    def on_localtime(self, target_time: time) -> Callable:
        """Decorator to schedule a function to run at a specific local time daily.

        Args:
            target_time: The time to run the function

        Returns:
            The decorator function
        """

        def decorator(func: Callable) -> Callable:
            def job():
                func()
                # Schedule for next day
                tomorrow = datetime.now(self._timezone).date() + timedelta(days=1)
                next_event_dt = datetime.combine(tomorrow, target_time).replace(
                    tzinfo=self._timezone
                )
                self._pending_callbacks.append(Callback(next_event_dt, job))

            # Schedule first event
            now = datetime.now(self._timezone)
            next_event_time = datetime.combine(now.date(), target_time).replace(
                tzinfo=self._timezone
            )

            if next_event_time <= now:
                next_event_time = datetime.combine(
                    now.date() + timedelta(days=1), target_time
                ).replace(tzinfo=self._timezone)

            self._pending_callbacks.append(Callback(next_event_time, job))
            return func

        return decorator

    def _get_sun_events(self):
        """Helper to get the last and next sun events."""
        now = datetime.now(self._timezone)
        today_dt = datetime.combine(now.date(), time(12, 0))
        yesterday_dt = today_dt - timedelta(days=1)
        tomorrow_dt = today_dt + timedelta(days=1)

        rise_today = self._get_sunrise_time(today_dt)
        set_today = self._get_sunset_time(today_dt)

        if rise_today > now:
            next_sunrise = rise_today
            last_sunrise = self._get_sunrise_time(yesterday_dt)
        else:
            last_sunrise = rise_today
            next_sunrise = self._get_sunrise_time(tomorrow_dt)

        if set_today > now:
            next_sunset = set_today
            last_sunset = self._get_sunset_time(yesterday_dt)
        else:
            last_sunset = set_today
            next_sunset = self._get_sunset_time(tomorrow_dt)

        return now, last_sunrise, next_sunrise, last_sunset, next_sunset

    def is_day(self) -> bool:
        """Returns True if it's currently daytime."""
        _, last_sunrise, _, last_sunset, _ = self._get_sun_events()
        return last_sunrise > last_sunset

    def is_before_sunrise(self) -> bool:
        """Returns True if it's currently before sunrise on the current day."""
        now, _, next_sunrise, _, _ = self._get_sun_events()
        return now.date() == next_sunrise.date() and now < next_sunrise

    def is_after_sunset(self) -> bool:
        """Returns True if it's currently after sunset on the current day."""
        now, _, _, last_sunset, _ = self._get_sun_events()
        return now.date() == last_sunset.date() and now > last_sunset

    def time_since_sunrise(self) -> timedelta:
        """Time since the last sunrise during the day and a negative time to the next sunrise during the night."""
        now, last_sunrise, next_sunrise, _, _ = self._get_sun_events()
        return now - last_sunrise if self.is_day() else now - next_sunrise

    def time_until_sunrise(self) -> timedelta:
        """Time until the next sunrise during the night and a negative time since the last sunrise during the day."""
        now, last_sunrise, next_sunrise, _, _ = self._get_sun_events()
        return next_sunrise - now if not self.is_day() else last_sunrise - now

    def time_since_sunset(self) -> timedelta:
        """Time since the last sunset during the night and a negative time to the next sunset during the day."""
        now, _, _, last_sunset, next_sunset = self._get_sun_events()
        return now - last_sunset if not self.is_day() else now - next_sunset

    def time_until_sunset(self) -> timedelta:
        """Time until the next sunset during the day and a negative time since the last sunset during the night."""
        now, _, _, last_sunset, next_sunset = self._get_sun_events()
        return next_sunset - now if self.is_day() else last_sunset - now

    def stop(self):
        """Stop the background scheduler."""
        self._running = False
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join()

    def __del__(self):
        """Cleanup when the object is destroyed."""
        self.stop()
