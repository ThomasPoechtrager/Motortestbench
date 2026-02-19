from dataclasses import dataclass, field
from threading import Lock
from typing import Optional

from config import MODE_TORQUE


@dataclass
class DriveCommand:
    mode: int = MODE_TORQUE
    target_velocity_rpm: int = 0
    target_torque_mnm: int = 0  # stored as mkg (kg Ã— 1000) for precision
    target_position_cm: int = 0
    profile_velocity_rpm: int = 500
    direction: int = -1
    cycle_time_ms: int = 20
    torque_slope: int = 50
    velocity_limit_rpm: int = 60


@dataclass
class DriveFeedback:
    position_cm: float = 0.0
    speed_cm_s: float = 0.0
    speed_rpm: float = 0.0
    torque_mnm: float = 0.0
    current_a: float = 0.0
    dc_bus_voltage: float = 0.0
    dc_bus_voltage_max: float = 0.0
    drive_temperature: float = 0.0
    chassis_temperature: float = 0.0
    statusword: int = 0
    error_code: int = 0
    mode_display: int = 0
    last_tpdo: Optional[int] = None


@dataclass
class DriveFlags:
    connected: bool = False
    running: bool = False
    fault: bool = False
    target_reached: bool = False
    request_connect: bool = False
    request_disconnect: bool = False
    request_start: bool = False
    request_stop: bool = False
    request_fault_reset: bool = False


@dataclass
class DriveState:
    command: DriveCommand = field(default_factory=DriveCommand)
    feedback: DriveFeedback = field(default_factory=DriveFeedback)
    flags: DriveFlags = field(default_factory=DriveFlags)
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def update_command(self, **kwargs) -> None:
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.command, key):
                    setattr(self.command, key, value)

    def update_feedback(self, **kwargs) -> None:
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.feedback, key):
                    setattr(self.feedback, key, value)

    def update_flags(self, **kwargs) -> None:
        with self._lock:
            for key, value in kwargs.items():
                if hasattr(self.flags, key):
                    setattr(self.flags, key, value)

    def snapshot(self) -> "DriveState":
        with self._lock:
            return DriveState(
                command=DriveCommand(**self.command.__dict__),
                feedback=DriveFeedback(**self.feedback.__dict__),
                flags=DriveFlags(**self.flags.__dict__),
            )
