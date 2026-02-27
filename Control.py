# pylint: disable=invalid-name
"""
Control Layer - Intermediate layer between GUI and Drive
Reads from gui_data and writes to drive_data
Can implement internal control loops independent of GUI inputs
Prepared for multi-motor: drives dict of {node_id: DriveState}
"""

from typing import Dict
from PySide6 import QtCore
from gui_data import GuiState
from drive_data import DriveState
from config import CONTROL_INTERVAL_MS


class ControlLayer(QtCore.QObject):
    """
    Control layer that processes GUI commands and forwards them to the drive(s).
    Currently passes data through 1:1, but can be extended with internal loops.
    Supports multiple motors via drive_states dict.
    """

    status = QtCore.Signal(str)

    def __init__(self, gui_states: Dict[int, GuiState], drive_states: Dict[int, DriveState]):
        super().__init__()
        self._gui_states = gui_states
        self._drive_states = drive_states  # Dict of {node_id: DriveState}
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

    @QtCore.Slot()
    def shutdown(self):
        self.stop_control()
        if self._timer is not None:
            self._timer.deleteLater()
            self._timer = None

    def _control_cycle(self):
        """
        Main control cycle - currently just passes data through.
        Future: Add internal control loops, filtering, rate limiting, etc.
        Processes gui_state for all connected motors.
        """
        for node_id, drive_state in self._drive_states.items():
            gui_state = self._gui_states.get(node_id)
            if gui_state is None:
                continue
            gui_snapshot = gui_state.snapshot()

            drive_state.update_command(
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

            if gui_snapshot.flags.request_connect:
                drive_state.update_flags(request_connect=True)
                gui_state.update_flags(request_connect=False)
            if gui_snapshot.flags.request_disconnect:
                drive_state.update_flags(request_disconnect=True)
                gui_state.update_flags(request_disconnect=False)
            if gui_snapshot.flags.request_start:
                drive_state.update_flags(request_start=True)
                gui_state.update_flags(request_start=False)
            if gui_snapshot.flags.request_stop:
                drive_state.update_flags(request_stop=True)
                gui_state.update_flags(request_stop=False)
            if gui_snapshot.flags.request_fault_reset:
                drive_state.update_flags(request_fault_reset=True)
                gui_state.update_flags(request_fault_reset=False)

            drive_snapshot = drive_state.snapshot()

            gui_state.update_feedback(
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

            gui_state.update_flags(
                connected=drive_snapshot.flags.connected,
                running=drive_snapshot.flags.running,
                fault=drive_snapshot.flags.fault,
                target_reached=drive_snapshot.flags.target_reached,
            )
