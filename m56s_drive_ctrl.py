import struct
import serial
from PySide6 import QtCore

from config import (
    COM_PORT, SERIAL_BAUD, CAN_BITRATE_CODE, NODE_ID,
    COB_NMT, COB_SDO_RX_BASE, COB_RPDO1_BASE, COB_RPDO2_BASE, COB_RPDO3_BASE, COB_RPDO4_BASE,
    CW_SHUTDOWN, CW_SWITCH_ON, CW_ENABLE_OPERATION, CW_HALT_BIT, CW_DISABLE_VOLTAGE,
    MODE_POSITION, MODE_TORQUE, MODE_VELOCITY,
    PROFILE_ACCEL_DEFAULT, PROFILE_DECEL_DEFAULT, QUICK_STOP_DECEL_DEFAULT,
    MAX_PROFILE_VELOCITY_DEFAULT,
    TORQUE_INPUT_SCALE, POLL_INTERVAL_MS, TX_INTERVAL_MS
)
from scale import (
    rpm_to_raw, rpm_to_pulses_per_sec, cm_to_counts,
    torque_nm_to_raw, torque_raw_to_nm, current_raw_to_a,
    counts_to_cm, inc_per_sec_to_cm_s, inc_per_sec_to_rpm,
    RATED_CURRENT_A, RATED_TORQUE_NM
)
from drive_data import DriveState


def checksum_low8(data: bytes) -> int:
    return sum(data) & 0xFF


def build_can_frame_fixed20(can_id: int, data: bytes) -> bytes:
    if len(data) > 8:
        raise ValueError("CAN data length > 8")

    f_type = 0x01
    f_format = 0x01
    dlc = len(data) & 0x0F
    data_padded = data + bytes(8 - len(data))

    frame = bytearray(20)
    frame[0] = 0xAA
    frame[1] = 0x55
    frame[2] = 0x01
    frame[3] = f_type
    frame[4] = f_format
    frame[5:9] = can_id.to_bytes(4, byteorder="little", signed=False)
    frame[9] = dlc
    frame[10:18] = data_padded
    frame[18] = 0x00
    frame[19] = checksum_low8(frame[2:19])
    return bytes(frame)


def build_config_cmd_fixed20(
    can_bitrate_code: int,
    frame_type: int = 0x01,
    filter_id: int = 0x00000000,
    mask_id: int = 0x00000000,
    can_mode: int = 0x00,
    auto_retx: int = 0x00
) -> bytes:
    frame = bytearray(20)
    frame[0] = 0xAA
    frame[1] = 0x55
    frame[2] = 0x12
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


class DriveController(QtCore.QObject):
    status = QtCore.Signal(str)

    def __init__(self, state: DriveState):
        super().__init__()
        self._state = state
        self.ser = None
        self.running = False
        self.tx_interval_ms = TX_INTERVAL_MS
        self._timer = None
        self._tx_timer = None
        self._rx_buf = bytearray()
        self._last_torque = None
        self._pp_setpoint_pending = False
        self._pp_setpoint_pulse = 0
        self._pp_setpoint_delay = 0
        self._pp_wait_ack = False
        self._pp_ack_timeout = 0
        self._last_target_position = None
        self._last_torque_slope = None
        self._last_velocity_limit_raw = None
        self._last_mode = None

    def _ensure_timers(self):
        if self._timer is None:
            self._timer = QtCore.QTimer()
            self._timer.setInterval(POLL_INTERVAL_MS)
            self._timer.timeout.connect(self._poll)
        if self._tx_timer is None:
            self._tx_timer = QtCore.QTimer()
            self._tx_timer.setInterval(self.tx_interval_ms)
            self._tx_timer.timeout.connect(self._send_cyclic)

    @QtCore.Slot()
    def start_polling(self):
        """Start poll timer when thread starts"""
        self._ensure_timers()
        if self._timer and not self._timer.isActive():
            self._timer.start()
            self.status.emit("Polling started")

    @QtCore.Slot()
    def connect_bus(self):
        try:
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

            cmd = self._state.snapshot().command
            sdo_write_u8(self.ser, 0x6060, 0x00, int(cmd.mode))

            # Profile position defaults
            sdo_write_u32(self.ser, 0x6083, 0x00, PROFILE_ACCEL_DEFAULT)
            sdo_write_u32(self.ser, 0x6084, 0x00, PROFILE_DECEL_DEFAULT)
            sdo_write_u32(self.ser, 0x6085, 0x00, QUICK_STOP_DECEL_DEFAULT)
            sdo_write_u32(self.ser, 0x607F, 0x00, MAX_PROFILE_VELOCITY_DEFAULT)

            velocity_limit_raw = rpm_to_raw(cmd.velocity_limit_rpm)
            rpdo2_send(
                self.ser,
                torque_slope=cmd.torque_slope,
                velocity_limit=velocity_limit_raw
            )
            self._last_torque_slope = cmd.torque_slope
            self._last_velocity_limit_raw = velocity_limit_raw
            self._last_mode = cmd.mode

            self._state.update_flags(connected=True)
            self.status.emit("Connected")
        except (serial.SerialException, OSError, ValueError) as e:
            self.status.emit(f"Connect error: {e}")

    @QtCore.Slot()
    def disconnect_bus(self):
        if self._tx_timer:
            self._tx_timer.stop()
        if self.ser:
            rpdo3_send(self.ser, target_velocity=0, target_position=0)
            rpdo1_send(self.ser, CW_SHUTDOWN, MODE_VELOCITY, target_torque=0)
            QtCore.QThread.msleep(20)
            rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, MODE_VELOCITY, target_torque=0)
            QtCore.QThread.msleep(150)
        if self.ser:
            try:
                self.ser.close()
            except (serial.SerialException, OSError):
                pass
            self.ser = None
        if self._tx_timer:
            self._tx_timer.stop()
        self.running = False
        self._state.update_flags(connected=False, running=False)
        self.status.emit("Disconnected")

    @QtCore.Slot()
    def start_motor(self):
        if not self.ser:
            self.status.emit("Not connected")
            return
        self._ensure_timers()
        cmd = self._state.snapshot().command
        rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SWITCH_ON, cmd.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_ENABLE_OPERATION, cmd.mode, target_torque=0)
        if self._tx_timer:
            self._tx_timer.setInterval(self.tx_interval_ms)
        self.running = True
        self._state.update_flags(running=True)
        if self._tx_timer:
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
        self._state.update_flags(running=False)
        self.status.emit("Stopped")

    @QtCore.Slot()
    def fault_reset(self):
        if not self.ser:
            self.status.emit("Not connected")
            return
        cw = CW_SHUTDOWN | 0x0080
        cmd = self._state.snapshot().command
        rpdo1_send(self.ser, cw, cmd.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
        self.status.emit("Fault reset sent")

    def _sync_cycle_time(self, cycle_time_ms: int):
        cycle_time_ms = max(1, int(cycle_time_ms))
        if cycle_time_ms != self.tx_interval_ms:
            self.tx_interval_ms = cycle_time_ms
            if self._tx_timer:
                self._tx_timer.setInterval(self.tx_interval_ms)

    def _send_disable_sequence(self):
        cw = CW_ENABLE_OPERATION | CW_HALT_BIT
        rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
        rpdo3_send(self.ser, target_velocity=0, target_position=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SHUTDOWN, MODE_VELOCITY, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, MODE_VELOCITY, target_torque=0)

    def _send_cyclic(self):
        if not self.ser or not self.running:
            return

        snapshot = self._state.snapshot()
        cmd = snapshot.command
        self._sync_cycle_time(cmd.cycle_time_ms)

        if self._last_mode != cmd.mode:
            sdo_write_u8(self.ser, 0x6060, 0x00, int(cmd.mode))
            self._last_mode = cmd.mode

        direction = 1 if int(cmd.direction) >= 0 else -1
        target_velocity = rpm_to_raw(cmd.target_velocity_rpm) * direction
        torque_nm = float(cmd.target_torque_mnm) / TORQUE_INPUT_SCALE
        target_torque = torque_nm_to_raw(torque_nm) * direction
        target_position = cm_to_counts(cmd.target_position_cm)
        profile_velocity = rpm_to_pulses_per_sec(cmd.profile_velocity_rpm)

        if cmd.target_position_cm != self._last_target_position:
            self._pp_setpoint_pending = True
            self._pp_ack_timeout = 50
            self._last_target_position = cmd.target_position_cm

        velocity_limit_raw = rpm_to_raw(cmd.velocity_limit_rpm)
        if (cmd.torque_slope != self._last_torque_slope or
                velocity_limit_raw != self._last_velocity_limit_raw):
            rpdo2_send(self.ser, cmd.torque_slope, velocity_limit_raw)
            self._last_torque_slope = cmd.torque_slope
            self._last_velocity_limit_raw = velocity_limit_raw

        cw = CW_ENABLE_OPERATION
        if cmd.mode == MODE_VELOCITY:
            rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
            rpdo3_send(self.ser, target_velocity=target_velocity, target_position=target_position)
        elif cmd.mode == MODE_POSITION:
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

            cw |= 0x0020
            cw &= ~0x0040
            if self._pp_setpoint_delay > 0:
                self._pp_setpoint_delay -= 1
            elif self._pp_setpoint_pulse > 0:
                cw |= 0x0010
                cw |= 0x0200
                self._pp_setpoint_pulse -= 1
            rpdo1_send(self.ser, cw, MODE_POSITION, target_torque=0)
            rpdo3_send(self.ser, target_velocity=target_velocity, target_position=target_position)
            rpdo4_send(self.ser, profile_velocity=profile_velocity)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=target_torque)
            rpdo3_send(self.ser, target_velocity=target_velocity, target_position=target_position)

    def _poll(self):
        # Check request flags first
        snapshot = self._state.snapshot()
        flags = snapshot.flags

        if flags.request_connect and not self.ser:
            self.connect_bus()
            self._state.update_flags(request_connect=False)

        if flags.request_disconnect and self.ser:
            self.disconnect_bus()
            self._state.update_flags(request_disconnect=False)

        if flags.request_fault_reset:
            self.fault_reset()
            self._state.update_flags(request_fault_reset=False)

        if flags.request_start and self.ser:
            self.start_motor()
            self._state.update_flags(request_start=False)

        if flags.request_stop and self.ser:
            self.stop_motor()
            self._state.update_flags(request_stop=False)

        if not self.ser:
            return
        pending = self.ser.in_waiting
        if pending:
            chunk = self.ser.read(pending)
            if chunk:
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

            if 0x700 <= can_id <= 0x77F and dlc >= 1:
                node_id = can_id - 0x700
                if node_id == NODE_ID:
                    state = data[0]
                    self.status.emit(f"HB state=0x{state:02X}")
            elif 0x180 <= can_id <= 0x1FF and dlc >= 5:
                node_id = can_id - 0x180
                if node_id == NODE_ID:
                    statusword, error_code, mode_disp = struct.unpack_from("<HHB", data, 0)
                    if statusword & 0x1000:
                        self._pp_wait_ack = False
                    fault = bool(statusword & (1 << 3))
                    target_reached = bool(statusword & (1 << 10))
                    self._state.update_feedback(
                        statusword=statusword,
                        error_code=error_code,
                        mode_display=mode_disp,
                        last_tpdo=can_id
                    )
                    self._state.update_flags(fault=fault, target_reached=target_reached)
            elif 0x280 <= can_id <= 0x2FF and dlc >= 8:
                node_id = can_id - 0x280
                if node_id == NODE_ID:
                    position, velocity = struct.unpack_from("<ii", data, 0)
                    pos_cm = counts_to_cm(position)
                    vel_cm_s = inc_per_sec_to_cm_s(velocity)
                    rpm = inc_per_sec_to_rpm(velocity)
                    self._state.update_feedback(
                        position_cm=pos_cm,
                        speed_cm_s=vel_cm_s,
                        speed_rpm=rpm,
                        last_tpdo=can_id
                    )
            elif 0x380 <= can_id <= 0x3FF and dlc >= 2:
                node_id = can_id - 0x380
                if node_id == NODE_ID:
                    current = struct.unpack_from("<h", data, 0)[0]
                    torque = None
                    if dlc >= 4:
                        torque = struct.unpack_from("<h", data, 2)[0]
                        self._last_torque = torque
                    else:
                        torque = self._last_torque
                    current_a = current_raw_to_a(current)
                    if torque is None:
                        torque_nm = (current_a / RATED_CURRENT_A) * RATED_TORQUE_NM
                    else:
                        torque_nm = torque_raw_to_nm(torque)
                    self._state.update_feedback(
                        current_a=current_a,
                        torque_mnm=torque_nm * 1000.0,
                        last_tpdo=can_id
                    )
