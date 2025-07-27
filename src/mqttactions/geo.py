import threading
import time as time_module
from collections import namedtuple
from datetime import datetime, timedelta, time
from typing import Callable, Optional
from timezonefinder import TimezoneFinder
from suntime import Sun
from zoneinfo import ZoneInfo

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
                next_sunrise = self._sun.get_sunrise_time(noon_tomorrow, self._timezone)
                self._pending_callbacks.append(Callback(next_sunrise + effective_offset, job))

            # Schedule first event
            now = datetime.now(self._timezone)
            noon_today = datetime.combine(now.date(), time(12, 0))
            next_event_time = self._sun.get_sunrise_time(noon_today, self._timezone) + effective_offset

            if next_event_time <= now:
                noon_tomorrow = datetime.combine(now.date() + timedelta(days=1), time(12, 0))
                next_event_time = (
                    self._sun.get_sunrise_time(noon_tomorrow, self._timezone) + effective_offset
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
                next_sunset = self._sun.get_sunset_time(noon_tomorrow, self._timezone)
                self._pending_callbacks.append(Callback(next_sunset + effective_offset, job))

            # Schedule first event
            now = datetime.now(self._timezone)
            noon_today = datetime.combine(now.date(), time(12, 0))
            next_event_time = self._sun.get_sunset_time(noon_today, self._timezone) + effective_offset

            if next_event_time <= now:
                noon_tomorrow = datetime.combine(now.date() + timedelta(days=1), time(12, 0))
                next_event_time = (
                    self._sun.get_sunset_time(noon_tomorrow, self._timezone) + effective_offset
                )

            self._pending_callbacks.append(Callback(next_event_time, job))
            return func

        return decorator

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

    def stop(self):
        """Stop the background scheduler."""
        self._running = False
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            self._scheduler_thread.join()

    def __del__(self):
        """Cleanup when the object is destroyed."""
        self.stop()
