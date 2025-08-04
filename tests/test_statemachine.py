import pytest
import time
import threading
from unittest.mock import Mock, patch
from mqttactions.statemachine import StateMachine, State


class TestStateMachine:

    def test_add_state(self):
        """Test adding states to state machine."""
        sm = StateMachine()

        # Add first state
        state1 = sm.add_state("idle")
        assert isinstance(state1, State)
        assert state1.name == "idle"
        assert "idle" in sm.states
        assert sm.get_current_state_name() == "idle"  # The first state becomes current

        # Add a second state
        state2 = sm.add_state("active")
        assert state2.name == "active"
        assert "active" in sm.states
        assert sm.get_current_state_name() == "idle"  # Current state unchanged

    def test_duplicate_state_raises_error(self):
        """Test that adding duplicate state raises error."""
        sm = StateMachine()
        sm.add_state("test")

        with pytest.raises(ValueError, match="State 'test' already exists"):
            sm.add_state("test")

    def test_transition_to_existing_state(self):
        """Test transitioning between states."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        assert sm.get_current_state_name() == "state1"

        sm.transition_to("state2")
        assert sm.get_current_state_name() == "state2"

    def test_transition_to_nonexistent_state_raises_error(self):
        """Test that transitioning to a non-existent state raises error."""
        sm = StateMachine()
        sm.add_state("existing")

        with pytest.raises(ValueError, match="State 'nonexistent' does not exist"):
            sm.transition_to("nonexistent")

    def test_transition_to_same_state(self):
        """Test that transitioning to the same state is handled gracefully."""
        sm = StateMachine()
        state1 = sm.add_state("state1")

        # Mock entry/exit methods to verify they're not called unnecessarily
        state1.enter = Mock()
        state1.exit = Mock()

        sm.transition_to("state1")  # Transition to the same state

        # Should not call entry/exit methods
        state1.enter.assert_not_called()
        state1.exit.assert_not_called()


class TestState:

    def test_entry_and_exit_callbacks(self):
        """Test entry and exit callback decorators."""
        sm = StateMachine()

        entry_mock = Mock()
        exit_mock = Mock()

        # Register callbacks before adding state
        state = sm.add_state("test")

        @state.on_entry
        def entry_func():
            entry_mock()

        @state.on_exit  
        def exit_func():
            exit_mock()

        # Verify callbacks are registered
        assert len(state._entry_callbacks) == 1
        assert len(state._exit_callbacks) == 1

        # Add another state and transition back to test state to trigger entry
        state2 = sm.add_state("state2")
        sm.transition_to("state2")
        sm.transition_to("test")  # This should trigger entry callback

        # Entry callback should have been called when we transitioned back
        entry_mock.assert_called_once()

        # Transition away to trigger exit - exit was called once when leaving test initially,
        # and will be called again when leaving test the second time
        sm.transition_to("state2")
        assert exit_mock.call_count == 2  # Called twice: first transition out and second transition out

    def test_multiple_entry_exit_callbacks(self):
        """Test multiple entry/exit callbacks on same state."""
        sm = StateMachine()
        state = sm.add_state("test")

        mock1 = Mock()
        mock2 = Mock()

        @state.on_entry
        def entry1():
            mock1()

        @state.on_entry
        def entry2():
            mock2()

        # Add another state and transition to trigger entry callbacks
        state2 = sm.add_state("state2")
        sm.transition_to("state2")
        sm.transition_to("test")  # This should trigger both entry callbacks

        # Both should be called
        mock1.assert_called_once()
        mock2.assert_called_once()

    @patch('mqttactions.statemachine.add_subscriber')
    def test_on_message_transition(self, mock_add_subscriber):
        """Test MQTT message-based transitions."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        # Set up transition
        state1.on_message("test/topic", "state2", payload_filter="trigger")

        # Verify subscriber was added
        mock_add_subscriber.assert_called_once()
        args = mock_add_subscriber.call_args[0]
        assert args[0] == "test/topic"  # topic

        # Get the callback that was registered
        callback = args[1]

        # Simulate message received while in state1
        assert sm.get_current_state_name() == "state1"
        callback(b"trigger")
        assert sm.get_current_state_name() == "state2"

    @patch('mqttactions.statemachine.add_subscriber')
    def test_on_message_transition_with_state_object(self, mock_add_subscriber):
        """Test MQTT transition using a State object instead of string."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        # Set up transition using a State object
        state1.on_message("test/topic", state2)

        # Get the callback that was registered
        callback = mock_add_subscriber.call_args[0][1]

        # Simulate message received
        callback(b"")
        assert sm.get_current_state_name() == "state2"

    @patch('mqttactions.statemachine.add_subscriber')
    def test_message_transition_only_from_current_state(self, mock_add_subscriber):
        """Test that message transitions only work from the current state."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")
        state3 = sm.add_state("state3")

        # Set up transition from state1 to state3
        state1.on_message("test/topic", "state3")
        callback = mock_add_subscriber.call_args[0][1]

        # Transition to state2 first
        sm.transition_to("state2")
        assert sm.get_current_state_name() == "state2"

        # Now trigger the message - should not transition because we're not in state1
        callback(b"")
        assert sm.get_current_state_name() == "state2"  # Should remain in state2

    def test_timeout_transition(self):
        """Test timeout-based transitions."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        # Set up a very short timeout
        state1.after_timeout(0.1, "state2")

        # Force re-entry to state1 to activate timeout
        sm.transition_to("state2")
        sm.transition_to("state1")

        # Should start in state1
        assert sm.get_current_state_name() == "state1"

        # Wait for timeout
        time.sleep(0.2)

        # Should have transitioned to state2
        assert sm.get_current_state_name() == "state2"

    def test_timeout_transition_with_state_object(self):
        """Test timeout transition using a State object."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        # Set up timeout using a State object
        state1.after_timeout(0.1, state2)

        # Force re-entry to state1 to activate timeout
        sm.transition_to("state2")
        sm.transition_to("state1")

        time.sleep(0.2)
        assert sm.get_current_state_name() == "state2"

    def test_timeout_cancelled_on_manual_transition(self):
        """Test that timeout is cancelled when manually transitioning."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")
        state3 = sm.add_state("state3")

        # Set up a longer timeout
        state1.after_timeout(0.5, "state3")

        # Manually transition before timeout
        time.sleep(0.1)
        sm.transition_to("state2")

        # Wait past the timeout period
        time.sleep(0.6)

        # Should still be in state2, not state3
        assert sm.get_current_state_name() == "state2"

    def test_method_chaining(self):
        """Test that state methods return self for chaining."""
        sm = StateMachine()
        state = sm.add_state("test")

        # Should be able to chain method calls
        result = (state
                  .on_message("topic1", "test")
                  .after_timeout(1.0, "test"))

        assert result is state

    def test_callback_exceptions_dont_break_state_machine(self):
        """Test that exceptions in callbacks don't break the state machine."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        @state1.on_exit
        def failing_exit():
            raise Exception("Test exception")

        @state2.on_entry  
        def failing_entry():
            raise Exception("Test exception")

        # Transition should still work despite exceptions
        sm.transition_to("state2")
        assert sm.get_current_state_name() == "state2"

    def test_multiple_timeout_transitions_raise_error(self):
        """Test that adding multiple timeout transitions raises an error."""
        sm = StateMachine()
        state = sm.add_state("test")

        # The first timeout should work
        state.after_timeout(1.0, "test")

        # Second timeout should raise error
        with pytest.raises(ValueError, match="State 'test' already has a timeout transition configured"):
            state.after_timeout(2.0, "test")

    def test_concurrent_transitions(self):
        """Test thread safety of transitions."""
        sm = StateMachine()
        state1 = sm.add_state("state1")
        state2 = sm.add_state("state2")

        # Create multiple threads trying to transition simultaneously
        def transition_worker():
            for _ in range(10):
                sm.transition_to("state2")
                time.sleep(0.001)
                sm.transition_to("state1")
                time.sleep(0.001)

        threads = [threading.Thread(target=transition_worker) for _ in range(3)]

        for t in threads:
            t.start()

        for t in threads:
            t.join()

        # Should end up in a valid state
        assert sm.get_current_state_name() in ["state1", "state2"]
