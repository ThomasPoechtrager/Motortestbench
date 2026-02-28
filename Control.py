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
from config import (
    CONTROL_INTERVAL_MS,
    CW_SHUTDOWN, CW_ENABLE_OPERATION, CW_DISABLE_VOLTAGE,
    MODE_POSITION
)


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
        # Runtime state per motor
        self._motor_state = {
            node_id: {
                "running": False,
                "controlword": CW_SHUTDOWN,
                "pp_setpoint_pending": False,
                "pp_setpoint_delay": 0,
                "pp_setpoint_pulse": 0,
                "last_position_setpoint_counter": -1,
            }
            for node_id in drive_states.keys()
        }

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
        Main control cycle - passes GUI data through and manages controlword state machine.
        Processes gui_state for all connected motors.
        """
        for node_id in sorted(self._drive_states.keys()):
            drive_state = self._drive_states[node_id]
            gui_state = self._gui_states.get(node_id)
            if gui_state is None:
                continue
            gui_snapshot = gui_state.snapshot()
            drive_snapshot = drive_state.snapshot()
            motor_state = self._motor_state[node_id]

            # Handle start request
            if gui_snapshot.flags.request_start and drive_snapshot.flags.connected:
                motor_state["running"] = True
                motor_state["controlword"] = CW_ENABLE_OPERATION
                gui_state.update_flags(request_start=False)
                motor_state["pp_setpoint_pending"] = False
                motor_state["last_position_setpoint_counter"] = -1

            # Handle stop request  
            if gui_snapshot.flags.request_stop:
                motor_state["running"] = False
                motor_state["controlword"] = CW_DISABLE_VOLTAGE
                gui_state.update_flags(request_stop=False)

            # Handle fault reset request
            if gui_snapshot.flags.request_fault_reset:
                drive_state.update_flags(request_fault_reset=True)
                gui_state.update_flags(request_fault_reset=False)

            # Handle connect/disconnect (pass through to drive layer)
            if gui_snapshot.flags.request_connect:
                drive_state.update_flags(request_connect=True)
                gui_state.update_flags(request_connect=False)
            if gui_snapshot.flags.request_disconnect:
                motor_state["running"] = False
                motor_state["controlword"] = CW_SHUTDOWN
                drive_state.update_flags(request_disconnect=True)
                gui_state.update_flags(request_disconnect=False)

            # Detect new position setpoint (independent of running state)
            if gui_snapshot.command.mode == MODE_POSITION:
                if gui_snapshot.command.position_setpoint_counter != motor_state["last_position_setpoint_counter"]:
                    motor_state["pp_setpoint_pending"] = True
                    motor_state["last_position_setpoint_counter"] = gui_snapshot.command.position_setpoint_counter
            
            # Build controlword based on running state
            if motor_state["running"]:
                cw = CW_ENABLE_OPERATION
                
                # Position mode special handling
                if gui_snapshot.command.mode == MODE_POSITION:
                    # Position mode bits
                    cw |= 0x0020  # Absolute positioning
                    cw &= ~0x0040  # Not relative
                    
                    # New setpoint bit pulsing (extended to 5 cycles for reliability)
                    if motor_state["pp_setpoint_pending"]:
                        motor_state["pp_setpoint_delay"] = 2
                        motor_state["pp_setpoint_pulse"] = 5
                        motor_state["pp_setpoint_pending"] = False
                    
                    if motor_state["pp_setpoint_delay"] > 0:
                        motor_state["pp_setpoint_delay"] -= 1
                    elif motor_state["pp_setpoint_pulse"] > 0:
                        cw |= 0x0010  # New setpoint
                        cw |= 0x0200  # Change immediately
                        motor_state["pp_setpoint_pulse"] -= 1
                
                motor_state["controlword"] = cw
            # else: keep last controlword (CW_DISABLE_VOLTAGE or CW_SHUTDOWN)

            drive_state.update_flags(running=motor_state["running"])

            # Position mode: keep pushing position even when not running to prep for next start
            # Velocity/Torque modes: always push targets
            # Write all command data including controlword to drive_state
            drive_state.update_command(
                mode=gui_snapshot.command.mode,
                target_velocity_rpm=gui_snapshot.command.target_velocity_rpm if motor_state["running"] else 0,
                target_torque_mnm=gui_snapshot.command.target_torque_mnm if motor_state["running"] else 0,
                target_position_cm=gui_snapshot.command.target_position_cm,  # Always push position
                position_setpoint_counter=gui_snapshot.command.position_setpoint_counter,
                profile_velocity_rpm=gui_snapshot.command.profile_velocity_rpm if motor_state["running"] else 0,
                direction=gui_snapshot.command.direction,
                cycle_time_ms=gui_snapshot.command.cycle_time_ms,
                torque_slope=gui_snapshot.command.torque_slope,
                velocity_limit_rpm=gui_snapshot.command.velocity_limit_rpm,
                controlword=motor_state["controlword"],
            )

            # Take fresh snapshot AFTER all drive_state updates to get consistent data
            fresh_drive_snapshot = drive_state.snapshot()

            # Update GUI with fresh feedback and flags
            gui_state.update_feedback(
                position_cm=fresh_drive_snapshot.feedback.position_cm,
                speed_cm_s=fresh_drive_snapshot.feedback.speed_cm_s,
                speed_rpm=fresh_drive_snapshot.feedback.speed_rpm,
                torque_mnm=fresh_drive_snapshot.feedback.torque_mnm,
                current_a=fresh_drive_snapshot.feedback.current_a,
                dc_bus_voltage=fresh_drive_snapshot.feedback.dc_bus_voltage,
                dc_bus_voltage_max=fresh_drive_snapshot.feedback.dc_bus_voltage_max,
                drive_temperature=fresh_drive_snapshot.feedback.drive_temperature,
                chassis_temperature=fresh_drive_snapshot.feedback.chassis_temperature,
                statusword=fresh_drive_snapshot.feedback.statusword,
                error_code=fresh_drive_snapshot.feedback.error_code,
                mode_display=fresh_drive_snapshot.feedback.mode_display,
                last_tpdo=fresh_drive_snapshot.feedback.last_tpdo,
            )

            gui_state.update_flags(
                connected=fresh_drive_snapshot.flags.connected,
                running=fresh_drive_snapshot.flags.running,
                fault=fresh_drive_snapshot.flags.fault,
                target_reached=fresh_drive_snapshot.flags.target_reached,
            )
