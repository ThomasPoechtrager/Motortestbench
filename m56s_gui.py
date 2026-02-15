import sys
from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg
import serial
import struct
import math
import time
from collections import deque

# CANopen/Waveshare configuration
COM_PORT = "COM17"
SERIAL_BAUD = 2_000_000  # Waveshare default serial baud
CAN_BITRATE_CODE = 0x01  # 1 Mbps
NODE_ID = 1

# CANopen COB-IDs
COB_NMT = 0x000
COB_SDO_RX_BASE = 0x600
COB_SDO_TX_BASE = 0x580
COB_RPDO1_BASE = 0x200
COB_RPDO2_BASE = 0x300
COB_RPDO3_BASE = 0x400
COB_RPDO4_BASE = 0x500
COB_TPDO1_BASE = 0x180
COB_HEARTBEAT = 0x700

# CiA402 Controlword
CW_SHUTDOWN = 0x0006
CW_SWITCH_ON = 0x0007
CW_ENABLE_OPERATION = 0x000F
CW_HALT_BIT = 0x0100  # bit8

# Modes of Operation
MODE_POSITION = 1
MODE_TORQUE = 4
MODE_VELOCITY = 3

def checksum_low8(data: bytes) -> int:
    return sum(data) & 0xFF

def build_can_frame_fixed20(can_id: int, data: bytes) -> bytes:
    if len(data) > 8:
        raise ValueError("CAN data length > 8")

    f_type = 0x01   # standard
    f_format = 0x01 # data
    dlc = len(data) & 0x0F
    data_padded = data + bytes(8 - len(data))

    frame = bytearray(20)
    frame[0] = 0xAA
    frame[1] = 0x55
    frame[2] = 0x01             # data frame type
    frame[3] = f_type
    frame[4] = f_format
    frame[5:9] = can_id.to_bytes(4, byteorder="little", signed=False)
    frame[9] = dlc
    frame[10:18] = data_padded
    frame[18] = 0x00
    frame[19] = checksum_low8(frame[2:19])
    return bytes(frame)

def build_config_cmd_fixed20(can_bitrate_code: int,
                             frame_type: int = 0x01,
                             filter_id: int = 0x00000000,
                             mask_id: int = 0x00000000,
                             can_mode: int = 0x00,
                             auto_retx: int = 0x00) -> bytes:
    frame = bytearray(20)
    frame[0] = 0xAA
    frame[1] = 0x55
    frame[2] = 0x12  # config frame type
    frame[3] = can_bitrate_code & 0xFF
    frame[4] = frame_type & 0xFF
    frame[5] = can_mode & 0xFF
    frame[6] = auto_retx & 0xFF
    frame[7:11] = filter_id.to_bytes(4, "little", signed=False)
    frame[11:15] = mask_id.to_bytes(4, "little", signed=False)
    frame[15] = 0x00
    frame[16] = 0x00
    frame[17] = 0x00
    frame[18] = 0x00
    frame[19] = checksum_low8(frame[2:19])
    return bytes(frame)

def send_can(ser: serial.Serial, can_id: int, data: bytes):
    frame = build_can_frame_fixed20(can_id, data)
    ser.write(frame)
    ser.flush()

def sdo_write_u8(ser, index, subindex, value):
    data = bytes([
        0x2F,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        value & 0xFF,
        0x00, 0x00, 0x00
    ])
    send_can(ser, COB_SDO_RX_BASE + NODE_ID, data)

def sdo_write_u16(ser, index, subindex, value):
    data = bytes([
        0x2B,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        value & 0xFF, (value >> 8) & 0xFF,
        0x00, 0x00
    ])
    send_can(ser, COB_SDO_RX_BASE + NODE_ID, data)

def sdo_write_u32(ser, index, subindex, value):
    data = bytes([
        0x23,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        value & 0xFF, (value >> 8) & 0xFF,
        (value >> 16) & 0xFF, (value >> 24) & 0xFF
    ])
    send_can(ser, COB_SDO_RX_BASE + NODE_ID, data)

def sdo_write_i32(ser, index, subindex, value):
    v = int(value).to_bytes(4, "little", signed=True)
    data = bytes([
        0x23,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        v[0], v[1], v[2], v[3]
    ])
    send_can(ser, COB_SDO_RX_BASE + NODE_ID, data)

def nmt_send(ser, command, node_id):
    send_can(ser, COB_NMT, bytes([command, node_id]))

def rpdo1_send(ser, controlword, mode, target_torque):
    data = bytearray(8)
    data[0:2] = int(controlword).to_bytes(2, "little", signed=False)
    data[2] = int(mode) & 0xFF
    data[3:5] = int(target_torque).to_bytes(2, "little", signed=True)
    data[5] = 0x00
    send_can(ser, COB_RPDO1_BASE + NODE_ID, bytes(data[:5]))

def rpdo2_send(ser, torque_slope, velocity_limit):
    data = bytearray(8)
    data[0:4] = int(torque_slope).to_bytes(4, "little", signed=True)
    data[4:8] = int(velocity_limit).to_bytes(4, "little", signed=True)
    send_can(ser, COB_RPDO2_BASE + NODE_ID, bytes(data))

def rpdo3_send(ser, target_velocity, target_position):
    data = bytearray(8)
    data[0:4] = int(target_velocity).to_bytes(4, "little", signed=True)
    data[4:8] = int(target_position).to_bytes(4, "little", signed=True)
    send_can(ser, COB_RPDO3_BASE + NODE_ID, bytes(data))

def rpdo4_send(ser, profile_velocity):
    data = int(profile_velocity).to_bytes(4, "little", signed=False)
    send_can(ser, COB_RPDO4_BASE + NODE_ID, data)

RPM_TO_RAW = 10000 / 60
INC_PER_REV = 10000
CW_DISABLE_VOLTAGE = 0x0000
SHAFT_RADIUS_CM = 6.0
GEAR_RATIO = 5.0  # motor revs per shaft rev
CM_PER_SHAFT_REV = 2 * math.pi * SHAFT_RADIUS_CM
RATED_TORQUE_NM = 2.4
RATED_CURRENT_A = 4.5
CURRENT_ACTUAL_PCT_PER_LSB = 0.1
TORQUE_PCT_PER_LSB = 0.1
TORQUE_INPUT_SCALE = 1000
TORQUE_SLOPE_SCALE = 1000
PROFILE_VELOCITY_DEFAULT = 800000
PROFILE_ACCEL_DEFAULT = 1000000
PROFILE_DECEL_DEFAULT = 1000000
QUICK_STOP_DECEL_DEFAULT = 30000000
MAX_PROFILE_VELOCITY_DEFAULT = 800000

def rpm_to_raw(rpm: float) -> int:
    return int(round(rpm * RPM_TO_RAW))

def inc_per_sec_to_rpm(inc_per_sec: int) -> int:
    return int((inc_per_sec * 60) / INC_PER_REV)

def rpm_to_pulses_per_sec(rpm: float) -> int:
    return int(round((rpm * INC_PER_REV) / 60))

def counts_to_cm(counts: int) -> float:
    return (counts / (INC_PER_REV * GEAR_RATIO)) * CM_PER_SHAFT_REV

def inc_per_sec_to_cm_s(inc_per_sec: int) -> float:
    return (inc_per_sec / (INC_PER_REV * GEAR_RATIO)) * CM_PER_SHAFT_REV

def cm_to_counts(cm: float) -> int:
    return int(round((cm / CM_PER_SHAFT_REV) * (INC_PER_REV * GEAR_RATIO)))

def torque_nm_to_raw(torque_nm: float) -> int:
    return int(round((torque_nm / RATED_TORQUE_NM) * (100 / TORQUE_PCT_PER_LSB)))

def torque_raw_to_nm(torque_raw: int) -> float:
    return (torque_raw * (TORQUE_PCT_PER_LSB / 100)) * RATED_TORQUE_NM

def current_raw_to_a(current_raw: int) -> float:
    return (current_raw * (CURRENT_ACTUAL_PCT_PER_LSB / 100)) * RATED_CURRENT_A

class CanWorker(QtCore.QObject):
    status = QtCore.Signal(str)
    tpdo1 = QtCore.Signal(int, int, int)
    tpdo2 = QtCore.Signal(int, int)
    tpdo3 = QtCore.Signal(int, object)
    frame_debug = QtCore.Signal(int, int, int)
    raw_frame = QtCore.Signal(int, int)
    raw_bytes = QtCore.Signal(int, int)

    def __init__(self):
        super().__init__()
        self.ser = None
        self.running = False
        self.mode = MODE_VELOCITY
        self.target_velocity = 0  # raw units
        self.target_torque = 0  # raw units
        self.target_position = 0  # counts
        self.profile_velocity = PROFILE_VELOCITY_DEFAULT  # pulses/s
        self.torque_slope = 5000
        self.velocity_limit = 10000
        self.direction = 1
        self.tx_interval_ms = 20
        self._timer = None
        self._tx_timer = None
        self._last_torque = None
        self._pp_setpoint_pending = False
        self._pp_setpoint_pulse = 0
        self._pp_setpoint_delay = 0
        self._pp_sdo_pending = False
        self._pp_wait_ack = False
        self._pp_ack_timeout = 0
        self._rx_count = 0
        self._rx_bytes = 0
        self._rx_buf = bytearray()

    def _ensure_timers(self):
        if self._timer is None:
            self._timer = QtCore.QTimer()
            self._timer.setInterval(50)
            self._timer.timeout.connect(self._poll)
        if self._tx_timer is None:
            self._tx_timer = QtCore.QTimer()
            self._tx_timer.setInterval(self.tx_interval_ms)
            self._tx_timer.timeout.connect(self._send_cyclic)

    @QtCore.Slot()
    def connect_bus(self):
        try:
            self._ensure_timers()
            self.ser = serial.Serial(COM_PORT, SERIAL_BAUD, timeout=0.05)
            cfg = build_config_cmd_fixed20(
                can_bitrate_code=CAN_BITRATE_CODE,
                frame_type=0x01,
                filter_id=0x00000000,
                mask_id=0x00000000,
                can_mode=0x00,
                auto_retx=0x00
            )
            self.ser.reset_input_buffer()
            self.ser.write(cfg)
            self.ser.flush()

            # Heartbeat producer time: 1000 ms
            sdo_write_u16(self.ser, 0x1017, 0x00, 1000)

            # Pre-Op -> Operational
            nmt_send(self.ser, 0x80, NODE_ID)
            QtCore.QThread.msleep(200)

            nmt_send(self.ser, 0x01, NODE_ID)
            QtCore.QThread.msleep(200)

            # Ensure mode of operation is set on connect
            sdo_write_u8(self.ser, 0x6060, 0x00, int(self.mode))

            # Profile position defaults
            sdo_write_u32(self.ser, 0x6083, 0x00, PROFILE_ACCEL_DEFAULT)
            sdo_write_u32(self.ser, 0x6084, 0x00, PROFILE_DECEL_DEFAULT)
            sdo_write_u32(self.ser, 0x6085, 0x00, QUICK_STOP_DECEL_DEFAULT)
            sdo_write_u32(self.ser, 0x607F, 0x00, MAX_PROFILE_VELOCITY_DEFAULT)

            # Default limits
            rpdo2_send(
                self.ser,
                torque_slope=self.torque_slope,
                velocity_limit=self.velocity_limit
            )

            self._timer.start()
            self.status.emit("Connected")
        except Exception as e:
            self.status.emit(f"Connect error: {e}")

    @QtCore.Slot()
    def disconnect_bus(self):
        if self._tx_timer:
            self._tx_timer.stop()
        if self.ser:
            if self.mode == MODE_VELOCITY:
                rpdo3_send(self.ser, target_velocity=0, target_position=0)
            elif self.mode == MODE_POSITION:
                rpdo3_send(self.ser, target_velocity=0, target_position=0)
            # Use shutdown/disable voltage only to avoid a brief enable
            rpdo1_send(self.ser, CW_SHUTDOWN, self.mode, target_torque=0)
            QtCore.QThread.msleep(20)
            rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, self.mode, target_torque=0)
            QtCore.QThread.msleep(150)
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
        if self._timer:
            self._timer.stop()
        if self._tx_timer:
            self._tx_timer.stop()
        self.status.emit("Disconnected")

    @QtCore.Slot(int)
    def set_mode(self, mode):
        self.mode = mode
        if self.ser:
            sdo_write_u8(self.ser, 0x6060, 0x00, int(mode))
        if mode == MODE_VELOCITY:
            mode_name = "Velocity"
        elif mode == MODE_TORQUE:
            mode_name = "Torque"
        else:
            mode_name = "Position"
        self.status.emit(f"Mode set: {mode_name}")

    @QtCore.Slot(int)
    def set_target_velocity(self, value):
        self.target_velocity = rpm_to_raw(value) * self.direction
        self.status.emit(f"Target velocity: {value} rpm -> {self.target_velocity} raw")

    @QtCore.Slot(int)
    def set_target_torque(self, value):
        torque_nm = float(value) / TORQUE_INPUT_SCALE
        self.target_torque = torque_nm_to_raw(torque_nm) * self.direction
        self.status.emit(f"Target torque: {torque_nm:.3f} Nm -> {self.target_torque} raw")

    @QtCore.Slot(int)
    def set_target_position(self, value):
        cm = float(value)
        self.target_position = cm_to_counts(cm)
        self._pp_setpoint_pending = True
        self._pp_sdo_pending = False
        self._pp_ack_timeout = 50
        self.status.emit(f"Target position: {cm:.2f} cm -> {self.target_position} counts")

    @QtCore.Slot(int)
    def set_profile_velocity(self, value):
        rpm = float(value)
        self.profile_velocity = max(0, rpm_to_pulses_per_sec(rpm))
        self._pp_sdo_pending = False
        self.status.emit(
            f"Profile velocity: {rpm:.0f} rpm -> {self.profile_velocity} pulses/s"
        )

    @QtCore.Slot(int)
    def set_torque_slope(self, value):
        self.torque_slope = int(value)
        if self.ser:
            rpdo2_send(self.ser, torque_slope=self.torque_slope, velocity_limit=self.velocity_limit)
            self.status.emit(f"Torque slope set: {self.torque_slope}")

    @QtCore.Slot(int)
    def set_velocity_limit(self, value):
        self.velocity_limit = rpm_to_raw(value)
        if self.ser:
            rpdo2_send(self.ser, torque_slope=self.torque_slope, velocity_limit=self.velocity_limit)
            self.status.emit(f"Velocity limit set: {value} rpm -> {self.velocity_limit} raw")

    @QtCore.Slot(int)
    def set_cycle_time(self, value):
        self.tx_interval_ms = max(1, int(value))
        if self._tx_timer:
            self._tx_timer.setInterval(self.tx_interval_ms)
        self.status.emit(f"Cycle time set: {self.tx_interval_ms} ms")

    @QtCore.Slot(int)
    def set_direction(self, value):
        self.direction = 1 if int(value) >= 0 else -1
        self.status.emit(f"Direction set: {'CW' if self.direction > 0 else 'CCW'}")

    @QtCore.Slot()
    def start_motor(self):
        if not self.ser:
            self.status.emit("Not connected")
            return
        self._ensure_timers()
        # Enable drive via PDO controlword sequence
        rpdo1_send(self.ser, CW_SHUTDOWN, self.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SWITCH_ON, self.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_ENABLE_OPERATION, self.mode, target_torque=0)
        self._tx_timer.setInterval(self.tx_interval_ms)
        self.running = True
        self._tx_timer.start()
        self.status.emit("Started")

    @QtCore.Slot()
    def stop_motor(self):
        if not self.ser:
            return
        if self._tx_timer:
            self._tx_timer.stop()
        self._send_disable_sequence()
        self.running = False
        self.status.emit("Stopped")

    @QtCore.Slot()
    def fault_reset(self):
        if not self.ser:
            self.status.emit("Not connected")
            return
        cw = CW_SHUTDOWN | 0x0080
        rpdo1_send(self.ser, cw, self.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SHUTDOWN, self.mode, target_torque=0)
        self.status.emit("Fault reset sent")

    def _send_disable_sequence(self):
        cw = CW_ENABLE_OPERATION | CW_HALT_BIT
        if self.mode == MODE_VELOCITY:
            rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
            rpdo3_send(self.ser, target_velocity=0, target_position=0)
        elif self.mode == MODE_POSITION:
            rpdo1_send(self.ser, cw, MODE_POSITION, target_torque=0)
            rpdo3_send(self.ser, target_velocity=0, target_position=0)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=0)
            rpdo3_send(self.ser, target_velocity=0, target_position=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SHUTDOWN, self.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, self.mode, target_torque=0)

    def _send_cyclic(self):
        if not self.ser or not self.running:
            return
        cw = CW_ENABLE_OPERATION
        if self.mode == MODE_VELOCITY:
            rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
            rpdo3_send(self.ser, target_velocity=self.target_velocity, target_position=self.target_position)
        elif self.mode == MODE_POSITION:
            if self._pp_wait_ack and self._pp_ack_timeout > 0:
                self._pp_ack_timeout -= 1
                if self._pp_ack_timeout == 0:
                    self._pp_wait_ack = False
                    self.status.emit("PP ack timeout, retrying setpoint")
            if self._pp_setpoint_pending and not self._pp_wait_ack:
                self._pp_setpoint_delay = 1
                self._pp_setpoint_pulse = 2
                self._pp_setpoint_pending = False
                self._pp_wait_ack = True
            cw |= 0x0020  # Change set immediately (bit 5)
            cw &= ~0x0040  # Absolute positioning (bit 6 = 0)
            if self._pp_setpoint_delay > 0:
                self._pp_setpoint_delay -= 1
            elif self._pp_setpoint_pulse > 0:
                cw |= 0x0010  # New set-point (bit 4)
                cw |= 0x0200  # Change of set point (bit 9)
                self._pp_setpoint_pulse -= 1
            rpdo1_send(self.ser, cw, MODE_POSITION, target_torque=0)
            rpdo3_send(self.ser, target_velocity=self.target_velocity, target_position=self.target_position)
            rpdo4_send(self.ser, profile_velocity=self.profile_velocity)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=self.target_torque)
            rpdo3_send(self.ser, target_velocity=self.target_velocity, target_position=self.target_position)

    def _poll(self):
        if not self.ser:
            return
        pending = self.ser.in_waiting
        if pending:
            chunk = self.ser.read(pending)
            if chunk:
                self._rx_bytes += len(chunk)
                self.raw_bytes.emit(self._rx_bytes, len(chunk))
                self._rx_buf.extend(chunk)

        while True:
            idx_aa55 = self._rx_buf.find(b"\xAA\x55")
            idx_55aa = self._rx_buf.find(b"\x55\xAA")
            if idx_aa55 < 0 and idx_55aa < 0:
                if len(self._rx_buf) > 64:
                    self._rx_buf.clear()
                break
            if idx_aa55 >= 0 and idx_55aa >= 0:
                idx = min(idx_aa55, idx_55aa)
            else:
                idx = idx_aa55 if idx_aa55 >= 0 else idx_55aa
            if idx > 0:
                del self._rx_buf[:idx]
            if len(self._rx_buf) < 2:
                break

            if self._rx_buf[:2] == b"\x55\xAA":
                if len(self._rx_buf) < 5:
                    break
                type_dlc = self._rx_buf[2]
                dlc = type_dlc & 0x0F
                if dlc > 8:
                    del self._rx_buf[:1]
                    continue
                frame_len = 2 + 1 + 2 + dlc + 1
                if len(self._rx_buf) < frame_len:
                    break
                can_id = self._rx_buf[3] | (self._rx_buf[4] << 8)
                data_start = 5
                data_end = data_start + dlc
                data = bytes(self._rx_buf[data_start:data_end])
                del self._rx_buf[:frame_len]
            else:
                if len(self._rx_buf) < 20:
                    break
                frame = bytes(self._rx_buf[:20])
                del self._rx_buf[:20]
                can_id = int.from_bytes(frame[5:9], byteorder="little", signed=False)
                dlc = frame[9] & 0x0F
                data = frame[10:10 + dlc]

            self._rx_count += 1
            self.raw_frame.emit(can_id, dlc)
            if 0x700 <= can_id <= 0x77F and dlc >= 1:
                node_id = can_id - 0x700
                if node_id == NODE_ID:
                    state = data[0]
                    self.status.emit(f"HB state=0x{state:02X}")
            elif 0x180 <= can_id <= 0x1FF and dlc >= 5:
                node_id = can_id - 0x180
                self.frame_debug.emit(can_id, node_id, dlc)
                if node_id == NODE_ID:
                    statusword, error_code, mode_disp = struct.unpack_from("<HHB", data, 0)
                    if statusword & 0x1000:
                        self._pp_wait_ack = False
                    self.tpdo1.emit(statusword, error_code, mode_disp)
            elif 0x280 <= can_id <= 0x2FF and dlc >= 8:
                node_id = can_id - 0x280
                self.frame_debug.emit(can_id, node_id, dlc)
                if node_id == NODE_ID:
                    position, velocity = struct.unpack_from("<ii", data, 0)
                    self.tpdo2.emit(position, velocity)
            elif 0x380 <= can_id <= 0x3FF and dlc >= 2:
                node_id = can_id - 0x380
                self.frame_debug.emit(can_id, node_id, dlc)
                if node_id == NODE_ID:
                    current = struct.unpack_from("<h", data, 0)[0]
                    torque = None
                    if dlc >= 4:
                        torque = struct.unpack_from("<h", data, 2)[0]
                        self._last_torque = torque
                    else:
                        torque = self._last_torque
                    self.tpdo3.emit(current, torque)

class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("M56S Control (PySide6 + pyqtgraph)")
        self._status_leds = {}

        # UI
        self.mode_combo = QtWidgets.QComboBox()
        self.mode_combo.addItem("Velocity", MODE_VELOCITY)
        self.mode_combo.addItem("Torque", MODE_TORQUE)
        self.mode_combo.addItem("Position", MODE_POSITION)

        self.target_velocity_label = QtWidgets.QLabel("Target velocity (rpm)")
        self.target_velocity_spin = QtWidgets.QSpinBox()
        self.target_velocity_spin.setRange(-100000, 100000)
        self.target_velocity_spin.setValue(1000)
        self.target_velocity_spin.setKeyboardTracking(False)

        self.target_torque_label = QtWidgets.QLabel("Target torque (mNm)")
        self.target_torque_spin = QtWidgets.QSpinBox()
        self.target_torque_spin.setRange(-10000, 10000)
        self.target_torque_spin.setValue(0)
        self.target_torque_spin.setKeyboardTracking(False)

        self.target_position_label = QtWidgets.QLabel("Target position (cm)")
        self.target_position_spin = QtWidgets.QDoubleSpinBox()
        self.target_position_spin.setRange(-10000, 10000)
        self.target_position_spin.setDecimals(0)
        self.target_position_spin.setSingleStep(1)
        self.target_position_spin.setValue(0)
        self.target_position_spin.setKeyboardTracking(False)

        self.profile_velocity_label = QtWidgets.QLabel("Profile velocity (rpm)")
        self.profile_velocity_spin = QtWidgets.QSpinBox()
        self.profile_velocity_spin.setRange(0, 100000)
        self.profile_velocity_spin.setValue(500)
        self.profile_velocity_spin.setKeyboardTracking(False)

        self.torque_slope_spin = QtWidgets.QSpinBox()
        self.torque_slope_spin.setRange(0, 1_000_000)
        self.torque_slope_spin.setValue(5000)

        self.velocity_limit_spin = QtWidgets.QSpinBox()
        self.velocity_limit_spin.setRange(0, 10000)
        self.velocity_limit_spin.setValue(60)

        self.cycle_time_spin = QtWidgets.QSpinBox()
        self.cycle_time_spin.setRange(1, 1000)
        self.cycle_time_spin.setValue(20)
        self.cycle_time_spin.setKeyboardTracking(False)

        self.direction_combo = QtWidgets.QComboBox()
        self.direction_combo.addItem("CW", 1)
        self.direction_combo.addItem("CCW", -1)

        self.btn_connect = QtWidgets.QPushButton("Connect")
        self.btn_disconnect = QtWidgets.QPushButton("Disconnect")
        self.btn_start = QtWidgets.QPushButton("Start")
        self.btn_stop = QtWidgets.QPushButton("Stop")
        self.btn_fault_reset = QtWidgets.QPushButton("Fault reset")

        self.status = QtWidgets.QLabel("Disconnected")
        self.tpdo1_label = QtWidgets.QLabel("TPDO1: status=0x---- err=0x---- mode=--")
        self.tpdo2_label = QtWidgets.QLabel("TPDO2: pos=0 vel=0 rpm")
        self.tpdo3_label = QtWidgets.QLabel("TPDO3: current=0 torque=0")
        self.statusword_label = QtWidgets.QLabel("Statusword: 0x----")
        self.status_bits_group = self._build_status_bits()

        # Plot
        self.plot = pg.PlotWidget(title="Telemetry")
        self.plot.setLabel("bottom", "Time", units="s")
        self.plot.showGrid(x=True, y=True, alpha=0.2)
        self._plot_start = time.monotonic()
        self._plot_window_s = 20.0
        self._plot_interval_s = 0.1
        self._last_plot_t = 0.0
        self._plot_paused = False
        self._t = deque(maxlen=20000)
        self._pos = deque(maxlen=20000)
        self._vel = deque(maxlen=20000)
        self._torque = deque(maxlen=20000)
        self._last_pos_cm = 0.0
        self._last_vel_cm_s = 0.0
        self._last_torque_mnm = 0.0
        self._plot_item = self.plot.getPlotItem()
        self._plot_item.setLabel("left", "Position", units="cm")
        self._plot_item.showAxis("right")
        self._plot_item.getAxis("right").setLabel("Speed", units="cm/s")

        self._vel_view = pg.ViewBox()
        self._plot_item.scene().addItem(self._vel_view)
        self._plot_item.getAxis("right").linkToView(self._vel_view)
        self._vel_view.setXLink(self._plot_item)

        self._torque_axis = pg.AxisItem(orientation="right")
        self._torque_axis.setLabel("Torque", units="mNm")
        self._plot_item.layout.addItem(self._torque_axis, 1, 3)

        self._torque_view = pg.ViewBox()
        self._plot_item.scene().addItem(self._torque_view)
        self._torque_axis.linkToView(self._torque_view)
        self._torque_view.setXLink(self._plot_item)

        self._plot_item.vb.sigResized.connect(self._update_viewboxes)
        self._update_viewboxes()

        self._curve_pos = self._plot_item.plot(pen=pg.mkPen("#5cb85c", width=2))
        self._curve_vel = pg.PlotCurveItem(pen=pg.mkPen("#5bc0de", width=2))
        self._vel_view.addItem(self._curve_vel)
        self._curve_torque = pg.PlotCurveItem(pen=pg.mkPen("#f0ad4e", width=2))
        self._torque_view.addItem(self._curve_torque)

        self.btn_reset_plot = QtWidgets.QPushButton("Reset plot")
        self.btn_pause_plot = QtWidgets.QPushButton("Pause plot")

        # Layout
        form = QtWidgets.QFormLayout()
        form.addRow("Mode", self.mode_combo)
        form.addRow(self.target_velocity_label, self.target_velocity_spin)
        form.addRow(self.target_torque_label, self.target_torque_spin)
        form.addRow(self.target_position_label, self.target_position_spin)
        form.addRow(self.profile_velocity_label, self.profile_velocity_spin)
        form.addRow("Torque slope (0.1%/s)", self.torque_slope_spin)
        form.addRow("Velocity limit (motor rpm)", self.velocity_limit_spin)
        form.addRow("Direction", self.direction_combo)
        form.addRow("Cycle time (ms)", self.cycle_time_spin)

        btns = QtWidgets.QHBoxLayout()
        btns.addWidget(self.btn_connect)
        btns.addWidget(self.btn_disconnect)
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)
        btns.addWidget(self.btn_fault_reset)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addLayout(btns)
        layout.addWidget(self.status)
        status_row = QtWidgets.QHBoxLayout()
        status_row.addWidget(self.status_bits_group)

        status_details = QtWidgets.QVBoxLayout()
        status_details.addWidget(self.tpdo1_label)
        status_details.addWidget(self.tpdo2_label)
        status_details.addWidget(self.tpdo3_label)
        status_details.addWidget(self.statusword_label)
        status_details.addStretch(1)

        status_row.addLayout(status_details)
        layout.addLayout(status_row)
        plot_controls = QtWidgets.QHBoxLayout()
        plot_controls.addWidget(self.btn_reset_plot)
        plot_controls.addWidget(self.btn_pause_plot)
        plot_controls.addStretch(1)
        layout.addLayout(plot_controls)
        layout.addWidget(self.plot)

        # Worker thread
        self.thread = QtCore.QThread(self)
        self.worker = CanWorker()
        self.worker.moveToThread(self.thread)
        self.thread.start()

        # Signals
        self.btn_connect.clicked.connect(self.worker.connect_bus)
        self.btn_disconnect.clicked.connect(self.worker.disconnect_bus)
        self.btn_start.clicked.connect(self.worker.start_motor)
        self.btn_stop.clicked.connect(self.worker.stop_motor)
        self.btn_fault_reset.clicked.connect(self.worker.fault_reset)
        self.mode_combo.currentIndexChanged.connect(self._mode_changed)
        self.target_velocity_spin.editingFinished.connect(self._target_velocity_committed)
        self.target_torque_spin.editingFinished.connect(self._target_torque_committed)
        self.target_position_spin.editingFinished.connect(self._target_position_committed)
        self.profile_velocity_spin.editingFinished.connect(self._profile_velocity_committed)
        self.torque_slope_spin.editingFinished.connect(self._torque_slope_committed)
        self.velocity_limit_spin.editingFinished.connect(self._velocity_limit_committed)
        self.cycle_time_spin.editingFinished.connect(self._cycle_committed)
        self.direction_combo.currentIndexChanged.connect(self._direction_changed)
        self.btn_reset_plot.clicked.connect(self._reset_plot)
        self.btn_pause_plot.clicked.connect(self._toggle_plot_pause)
        self.worker.status.connect(self.status.setText)
        self.worker.tpdo1.connect(self._on_tpdo1)
        self.worker.tpdo2.connect(self._on_tpdo2)
        self.worker.tpdo3.connect(self._on_tpdo3)

        # Init
        self._mode_changed(0)
        self._target_velocity_committed()
        self._target_torque_committed()
        self._target_position_committed()
        self._profile_velocity_committed()
        self._torque_slope_committed()
        self._velocity_limit_committed()
        self._cycle_committed()
        self.worker.set_direction(self.direction_combo.currentData())

    def _mode_changed(self, _):
        mode = self.mode_combo.currentData()
        self.worker.set_mode(mode)
        self.target_velocity_spin.setEnabled(mode in (MODE_VELOCITY, MODE_POSITION))
        self.target_torque_spin.setEnabled(True)
        self.target_position_spin.setEnabled(mode == MODE_POSITION)
        self.profile_velocity_spin.setEnabled(mode == MODE_POSITION)
        self._target_velocity_committed()
        self._target_torque_committed()
        self._target_position_committed()
        self._profile_velocity_committed()

    def _direction_changed(self, _):
        self.worker.set_direction(self.direction_combo.currentData())
        self._target_velocity_committed()
        self._target_torque_committed()

    def _target_velocity_committed(self):
        self.target_velocity_spin.interpretText()
        self.worker.set_target_velocity(self.target_velocity_spin.value())

    def _target_torque_committed(self):
        self.target_torque_spin.interpretText()
        self.worker.set_target_torque(self.target_torque_spin.value())

    def _target_position_committed(self):
        self.worker.set_target_position(self.target_position_spin.value())

    def _profile_velocity_committed(self):
        self.profile_velocity_spin.interpretText()
        self.worker.set_profile_velocity(self.profile_velocity_spin.value())

    def _cycle_committed(self):
        self.cycle_time_spin.interpretText()
        self.worker.set_cycle_time(self.cycle_time_spin.value())

    def _torque_slope_committed(self):
        self.torque_slope_spin.interpretText()
        self.worker.set_torque_slope(self.torque_slope_spin.value() // TORQUE_SLOPE_SCALE)

    def _velocity_limit_committed(self):
        self.velocity_limit_spin.interpretText()
        self.worker.set_velocity_limit(self.velocity_limit_spin.value())

    def _on_tpdo1(self, statusword: int, error_code: int, mode_disp: int):
        self.tpdo1_label.setText(
            f"TPDO1: status=0x{statusword:04X} err=0x{error_code:04X} mode={mode_disp}"
        )
        self.statusword_label.setText(f"Statusword: 0x{statusword:04X}")
        self._update_status_bits(statusword)

    def _on_tpdo2(self, position: int, velocity: int):
        rpm = int(inc_per_sec_to_rpm(velocity) / GEAR_RATIO)
        pos_cm = counts_to_cm(position)
        vel_cm_s = inc_per_sec_to_cm_s(velocity)
        self.tpdo2_label.setText(
            f"TPDO2: pos={pos_cm:.2f} cm vel={vel_cm_s:.2f} cm/s ({rpm} rpm)"
        )
        self._last_pos_cm = pos_cm
        self._last_vel_cm_s = vel_cm_s
        self._append_plot()

    def _on_tpdo3(self, current: int, torque):
        current_a = current_raw_to_a(current)
        if torque is None:
            torque_nm = (current_a / RATED_CURRENT_A) * RATED_TORQUE_NM
            torque_suffix = " (est)"
        else:
            torque_nm = torque_raw_to_nm(torque)
            torque_suffix = ""
        self.tpdo3_label.setText(
            f"TPDO3: current={current_a:.2f} A torque={torque_nm:.2f} Nm{torque_suffix}"
        )
        self._last_torque_mnm = torque_nm * 1000.0
        self._append_plot()

    def _append_plot(self):
        if self._plot_paused:
            return
        t = time.monotonic() - self._plot_start
        if (t - self._last_plot_t) < self._plot_interval_s:
            return
        self._last_plot_t = t
        base_t = max(0.0, t - self._plot_window_s)
        while self._t and self._t[0] < base_t:
            self._t.popleft()
            self._pos.popleft()
            self._vel.popleft()
            self._torque.popleft()
        self._t.append(t)
        self._pos.append(self._last_pos_cm)
        self._vel.append(self._last_vel_cm_s)
        self._torque.append(self._last_torque_mnm)
        t_rel = [ti - base_t for ti in self._t]
        self._curve_pos.setData(t_rel, list(self._pos))
        self._curve_vel.setData(t_rel, list(self._vel))
        self._curve_torque.setData(t_rel, list(self._torque))
        x_max = min(self._plot_window_s, t) if t > 0 else self._plot_window_s
        self._plot_item.setXRange(0.0, x_max, padding=0.0)

    def _update_viewboxes(self):
        self._vel_view.setGeometry(self._plot_item.vb.sceneBoundingRect())
        self._vel_view.linkedViewChanged(self._plot_item.vb, self._vel_view.XAxis)
        self._torque_view.setGeometry(self._plot_item.vb.sceneBoundingRect())
        self._torque_view.linkedViewChanged(self._plot_item.vb, self._torque_view.XAxis)

    def _reset_plot(self):
        self._plot_start = time.monotonic()
        self._last_plot_t = 0.0
        self._t.clear()
        self._pos.clear()
        self._vel.clear()
        self._torque.clear()
        self._curve_pos.clear()
        self._curve_vel.clear()
        self._curve_torque.clear()
        self._plot_item.setXRange(0.0, self._plot_window_s, padding=0.0)

    def _toggle_plot_pause(self):
        self._plot_paused = not self._plot_paused
        self.btn_pause_plot.setText("Resume plot" if self._plot_paused else "Pause plot")

    def _on_frame_debug(self, can_id: int, node_id: int, dlc: int):
        self.frame_debug_label.setText(f"Last TPDO: id=0x{can_id:03X} node={node_id} dlc={dlc}")

    def _on_raw_frame(self, can_id: int, dlc: int):
        self.rx_label.setText(f"RX frames: {self.worker._rx_count} last=0x{can_id:03X} dlc={dlc}")

    def _on_raw_bytes(self, total_bytes: int, pending: int):
        self.rx_bytes_label.setText(f"RX bytes: {total_bytes} pending={pending}")

    def _build_status_bits(self):
        group = QtWidgets.QGroupBox("Statusword bits")
        grid = QtWidgets.QGridLayout(group)

        bit_defs = [
            (0, "Ready to switch on"),
            (1, "Switched on"),
            (2, "Operation enabled"),
            (3, "Fault"),
            (5, "Quick stop"),
            (6, "Switch on disabled"),
            (7, "Warning"),
            (8, "Remote"),
            (9, "Target reached"),
            (10, "Internal limit"),
        ]

        for row, (bit, label) in enumerate(bit_defs):
            led = QtWidgets.QLabel()
            led.setFixedSize(12, 12)
            led.setStyleSheet("border: 1px solid #666; border-radius: 6px; background: #333;")
            text = QtWidgets.QLabel(f"b{bit}: {label}")
            text.setFixedWidth(180)
            grid.addWidget(led, row, 0)
            grid.addWidget(text, row, 1)
            self._status_leds[bit] = led

        grid.setColumnStretch(2, 0)
        return group

    def _update_status_bits(self, statusword: int):
        for bit, led in self._status_leds.items():
            is_set = bool(statusword & (1 << bit))
            if bit == 3:
                color = "#d9534f" if is_set else "#333"
            elif bit == 7:
                color = "#f0ad4e" if is_set else "#333"
            else:
                color = "#5cb85c" if is_set else "#333"
            led.setStyleSheet(
                f"border: 1px solid #666; border-radius: 6px; background: {color};"
            )

    def closeEvent(self, event):
        QtCore.QMetaObject.invokeMethod(
            self.worker,
            "disconnect_bus",
            QtCore.Qt.BlockingQueuedConnection
        )
        self.thread.quit()
        self.thread.wait(500)
        super().closeEvent(event)

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.resize(600, 400)
    w.show()
    sys.exit(app.exec())

if __name__ == "__main__":
    main()