import logging
import threading
from typing import Dict, Optional, Callable, Union
from mqttactions.runtime import add_subscriber
from mqttactions.payloadconversion import converter_by_type, ConvertedPayload

logger = logging.getLogger(__name__)


class State:
    """Represents a state in a state machine with transition capabilities."""

    def __init__(self, name: str, state_machine: 'StateMachine'):
        self.name = name
        self.state_machine = state_machine
        self._entry_callbacks = []
        self._exit_callbacks = []
        self._timeout_timer = None
        self._timeout_transition = None

    def on_message(self, topic: str, target_state: Union[str, 'State'],
                   payload_filter: Optional[Union[str, dict]] = None) -> 'State':
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

    def after_timeout(self, seconds: float, target_state: Union[str, 'State']) -> 'State':
        """Add a transition triggered after a timeout.

        Args:
            seconds: The timeout duration in seconds
            target_state: The state to transition to (name or State object)

        Returns:
            Self for method chaining

        Raises:
            ValueError: If a timeout transition is already configured for this state
        """
        target_state_name = target_state if isinstance(target_state, str) else target_state.name

        # Check if a timeout transition already exists
        if self._timeout_transition is not None:
            raise ValueError(f"State '{self.name}' already has a timeout transition configured")

        # Store the single timeout configuration for this state
        self._timeout_transition = (seconds, target_state_name)
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
            timeout_seconds, target_state_name = self._timeout_transition

            def timeout_handler():
                if self.state_machine.current_state == self:
                    self.state_machine.transition_to(target_state_name)

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
        self.states: Dict[str, State] = {}
        self.topics_watched = set()
        self.state_transitions: Dict[str, list[tuple[str, Optional[ConvertedPayload]]]] = {}
        self.current_state: Optional[State] = None
        self._lock = threading.Lock()

    def add_state(self, name: str) -> State:
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

    def transition_to(self, state_name: str):
        """Transition to the specified state.

        Args:
            state_name: The name of the state to transition to
        """
        with self._lock:
            if state_name not in self.states:
                raise ValueError(f"State '{state_name}' does not exist")

            logger.info(f'Transitioning to state "{state_name}"')
            target_state = self.states[state_name]

            if self.current_state == target_state:
                return  # Already in the target state

            # Exit the current state
            if self.current_state:
                self.current_state.exit()

            # Enter the new state
            self.current_state = target_state
            target_state.enter()

    def register_transition(self, source_state_name: str, target_state_name: str, topic: str,
                            payload_filter: Optional[ConvertedPayload] = None):
        if topic not in self.topics_watched:
            add_subscriber(topic, self.on_message)
            self.topics_watched.add(topic)

        self.state_transitions.setdefault(source_state_name, []).append((target_state_name, payload_filter))

    def on_message(self, payload: bytes):
        if self.current_state and self.current_state.name in self.state_transitions:
            for t, f in self.state_transitions[self.current_state.name]:
                if f is None:
                    self.transition_to(t)
                    break

                converted = converter_by_type[f.__class__](payload)
                if converted == f:
                    self.transition_to(t)
                    break

    def get_current_state(self) -> Optional[str]:
        """Get the name of the current state.

        Returns:
            The name of the current state or None if no states exist
        """
        return self.current_state.name if self.current_state else None
