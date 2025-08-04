import logging
import threading
from functools import partial, wraps, update_wrapper
from typing import Dict, Optional, Callable, Union, Tuple
from mqttactions.runtime import add_subscriber
from mqttactions.payloadconversion import converter_by_type, PayloadFilter, matches_filter, get_filter_type

logger = logging.getLogger(__name__)

# Type aliases to make code more readable
StateName = str
MqttTopic = str


class State:
    """Represents a state in a state machine with transition capabilities."""

    def __init__(self, name: str, state_machine: 'StateMachine'):
        self.name = name
        self.state_machine = state_machine
        self._entry_callbacks = []
        self._exit_callbacks = []
        self._timeout_timer = None
        self._timeout_transition: Optional[Tuple[float, Callable[[], 'State']]] = None

    def on_message(self, topic: str, target_state: Union[str, 'State'],
                   payload_filter: PayloadFilter = None) -> 'State':
        """Add a transition triggered by an MQTT message.

        Args:
            topic: The MQTT topic to listen to
            target_state: The state to transition to (name or State object)
            payload_filter: An optional payload filter

        Returns:
            Self for method chaining
        """
        target_state_name = target_state if isinstance(target_state, str) else target_state.name
        self.state_machine.register_transition(self.name, target_state_name, topic, payload_filter)
        return self

    def on_message_filtered(self, topic: str, target_state: Union[str, 'State']) -> Callable:
        """Decorator version of on_message."""
        target_state_name = target_state if isinstance(target_state, str) else target_state.name

        def decorator(func: Callable) -> Callable:
            self.state_machine.register_transition(self.name, target_state_name, topic, func)
            return func
        return decorator

    def after_timeout(self, seconds: float, target_state: Union[str, 'State', Callable[[], 'State']]) -> 'State':
        """Add a transition triggered after a timeout.

        Args:
            seconds: The timeout duration in seconds
            target_state: The state to transition to (name or State object)

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a timeout transition is already configured for this state
        """
        callback = target_state
        if not callable(callback):
            target_state_name = target_state if isinstance(target_state, str) else target_state.name
            callback = lambda: target_state_name

        # Check if a timeout transition already exists
        if self._timeout_transition is not None:
            raise ValueError(f"State '{self.name}' already has a timeout transition configured")

        # Store the single timeout configuration for this state
        self._timeout_transition = (seconds, callback)
        return self

    def on_entry(self, func: Callable) -> Callable:
        """Decorator to register a function to be called when entering this state."""
        self._entry_callbacks.append(func)
        return func

    def on_exit(self, func: Callable) -> Callable:
        """Decorator to register a function to be called when exiting this state."""
        self._exit_callbacks.append(func)
        return func

    def enter(self):
        """Internal method called when entering this state."""
        # Cancel any existing timeout timer
        if self._timeout_timer:
            self._timeout_timer.cancel()

        # Execute the entry callbacks
        for callback in self._entry_callbacks:
            try:
                callback()
            except Exception as e:
                # Log the error but don't stop the state machine
                logger.error(f"Error in entry callback for state {self.name}: {e}")

        # Set up the timeout transition if configured
        if self._timeout_transition is not None:
            timeout_seconds, callback = self._timeout_transition

            def timeout_handler():
                if self.state_machine.current_state == self:
                    self.state_machine.transition_to(callback())

            self._timeout_timer = threading.Timer(timeout_seconds, timeout_handler)
            self._timeout_timer.start()

    def exit(self):
        """Internal method called when exiting this state."""
        # Cancel the timeout timer
        if self._timeout_timer:
            self._timeout_timer.cancel()
            self._timeout_timer = None

        # Execute the exit callbacks
        for callback in self._exit_callbacks:
            try:
                callback()
            except Exception as e:
                # Log the error but don't stop the state machine
                logger.error(f"Error in exit callback for state {self.name}: {e}")


class StateMachine:
    """The main state machine class for managing states and transitions."""

    def __init__(self):
        self.states: Dict[StateName, State] = {}
        self.topics_watched = set()
        self.state_transitions: Dict[MqttTopic, Dict[StateName, list[tuple[StateName, PayloadFilter]]]] = {}
        self.current_state: Optional[State] = None
        self._lock = threading.Lock()

    def add_state(self, name: StateName) -> State:
        """Add a new state to the state machine.

        Args:
            name: The name of the state

        Returns:
            The created State object
        """
        if name in self.states:
            raise ValueError(f"State '{name}' already exists")

        state = State(name, self)
        self.states[name] = state

        # If this is the first state, make it the current state
        if self.current_state is None:
            self.current_state = state
            state.enter()

        return state

    def transition_to(self, target_state: Union[StateName, State]):
        """Transition to the specified state.

        Args:
            target_state: The name of the state to transition to
        """
        if isinstance(target_state, State):
            target_state = target_state.name

        with self._lock:
            if target_state not in self.states:
                raise ValueError(f"State '{target_state}' does not exist")

            logger.info(f'Transitioning to state "{target_state}"')
            target_state = self.states[target_state]

            if self.current_state == target_state:
                return  # Already in the target state

            # Exit the current state
            if self.current_state:
                self.current_state.exit()

            # Enter the new state
            self.current_state = target_state
            target_state.enter()

    def register_transition(self, source_state_name: StateName, target_state_name: StateName,
                            topic: MqttTopic, payload_filter: PayloadFilter = None):
        if topic not in self.topics_watched:
            callback = partial(self.on_message, topic)
            callback.__name__ = "StateMachine.on_message"
            add_subscriber(topic, callback)
            self.topics_watched.add(topic)

        self.state_transitions.setdefault(topic, {}).setdefault(source_state_name, []).append(
            (target_state_name, payload_filter))

    def on_message(self, topic: MqttTopic, payload: bytes):
        for target, pfilter in self.state_transitions.get(topic, {}).get(self.current_state.name, []):
            converted = converter_by_type[get_filter_type(pfilter)](payload)
            if matches_filter(converted, pfilter):
                self.transition_to(target)
                break

    def get_current_state(self) -> Optional[State]:
        """Get the current state."""
        with self._lock:
            return self.current_state

    def get_current_state_name(self) -> Optional[str]:
        """Get the name of the current state.

        Returns:
            The name of the current state or None if no states exist
        """
        with self._lock:
            return self.current_state.name if self.current_state else None
