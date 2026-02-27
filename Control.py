# pylint: disable=invalid-name
"""
Control Layer - Intermediate layer between GUI and Drive
Reads from gui_data and writes to drive_data
Can implement internal control loops independent of GUI inputs
"""

from PySide6 import QtCore
from gui_data import GuiState
from drive_data import DriveState
from config import CONTROL_INTERVAL_MS


class ControlLayer(QtCore.QObject):
    """
    Control layer that processes GUI commands and forwards them to the drive.
    Currently passes data through 1:1, but can be extended with internal loops.
    """

    status = QtCore.Signal(str)

    def __init__(self, gui_state: GuiState, drive_state: DriveState):
        super().__init__()
        self._gui_state = gui_state
        self._drive_state = drive_state
        self._timer = None
        self._control_interval_ms = CONTROL_INTERVAL_MS

    def _ensure_timer(self):
        if self._timer is None:
            self._timer = QtCore.QTimer()
            self._timer.setInterval(self._control_interval_ms)
            self._timer.timeout.connect(self._control_cycle)

    @QtCore.Slot()
    def start_control(self):
        """Start the control loop when thread starts"""
        self._ensure_timer()
        if self._timer and not self._timer.isActive():
            self._timer.start()
            self.status.emit("Control layer started")

    @QtCore.Slot()
    def stop_control(self):
        """Stop the control loop"""
        if self._timer and self._timer.isActive():
            self._timer.stop()
            self.status.emit("Control layer stopped")

    def _control_cycle(self):
        """
        Main control cycle - currently just passes data through.
        Future: Add internal control loops, filtering, rate limiting, etc.
        """
        # Get snapshot from GUI state
        gui_snapshot = self._gui_state.snapshot()

        # Pass through commands (1:1 for now)
        self._drive_state.update_command(
            mode=gui_snapshot.command.mode,
            target_velocity_rpm=gui_snapshot.command.target_velocity_rpm,
            target_torque_mnm=gui_snapshot.command.target_torque_mnm,
            target_position_cm=gui_snapshot.command.target_position_cm,
            position_setpoint_counter=gui_snapshot.command.position_setpoint_counter,
            profile_velocity_rpm=gui_snapshot.command.profile_velocity_rpm,
            direction=gui_snapshot.command.direction,
            cycle_time_ms=gui_snapshot.command.cycle_time_ms,
            torque_slope=gui_snapshot.command.torque_slope,
            velocity_limit_rpm=gui_snapshot.command.velocity_limit_rpm,
        )

        # Handle request flags with edge detection (only forward on rising edge)
        # This prevents continuous triggering when GUI keeps flag set
        if gui_snapshot.flags.request_connect:
            self._drive_state.update_flags(request_connect=True)
            self._gui_state.update_flags(request_connect=False)  # Reset in GUI

        if gui_snapshot.flags.request_disconnect:
            self._drive_state.update_flags(request_disconnect=True)
            self._gui_state.update_flags(request_disconnect=False)  # Reset in GUI

        if gui_snapshot.flags.request_start:
            self._drive_state.update_flags(request_start=True)
            self._gui_state.update_flags(request_start=False)  # Reset in GUI

        if gui_snapshot.flags.request_stop:
            self._drive_state.update_flags(request_stop=True)
            self._gui_state.update_flags(request_stop=False)  # Reset in GUI

        if gui_snapshot.flags.request_fault_reset:
            self._drive_state.update_flags(request_fault_reset=True)
            self._gui_state.update_flags(request_fault_reset=False)  # Reset in GUI

        # Get feedback from drive and pass to GUI
        drive_snapshot = self._drive_state.snapshot()

        self._gui_state.update_feedback(
            position_cm=drive_snapshot.feedback.position_cm,
            speed_cm_s=drive_snapshot.feedback.speed_cm_s,
            speed_rpm=drive_snapshot.feedback.speed_rpm,
            torque_mnm=drive_snapshot.feedback.torque_mnm,
            current_a=drive_snapshot.feedback.current_a,
            dc_bus_voltage=drive_snapshot.feedback.dc_bus_voltage,
            dc_bus_voltage_max=drive_snapshot.feedback.dc_bus_voltage_max,
            drive_temperature=drive_snapshot.feedback.drive_temperature,
            chassis_temperature=drive_snapshot.feedback.chassis_temperature,
            statusword=drive_snapshot.feedback.statusword,
            error_code=drive_snapshot.feedback.error_code,
            mode_display=drive_snapshot.feedback.mode_display,
            last_tpdo=drive_snapshot.feedback.last_tpdo,
        )

        self._gui_state.update_flags(
            connected=drive_snapshot.flags.connected,
            running=drive_snapshot.flags.running,
            fault=drive_snapshot.flags.fault,
            target_reached=drive_snapshot.flags.target_reached,
        )
