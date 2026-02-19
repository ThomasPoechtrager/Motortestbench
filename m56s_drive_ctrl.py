import struct
import serial
from PySide6 import QtCore

from config import (
    COM_PORT, SERIAL_BAUD, CAN_BITRATE_CODE, NODE_ID,
    COB_NMT, COB_SDO_RX_BASE, COB_RPDO1_BASE, COB_RPDO2_BASE, COB_RPDO3_BASE, COB_RPDO4_BASE,
    CW_SHUTDOWN, CW_SWITCH_ON, CW_ENABLE_OPERATION, CW_DISABLE_VOLTAGE,
    MODE_POSITION, MODE_TORQUE, MODE_VELOCITY,
    PROFILE_ACCEL_DEFAULT, PROFILE_DECEL_DEFAULT, QUICK_STOP_DECEL_DEFAULT,
    MAX_PROFILE_VELOCITY_DEFAULT,
    POLL_INTERVAL_MS, TX_INTERVAL_MS, WATCHDOG_ENABLED
)
from scale import (
    rpm_to_raw, rpm_to_pulses_per_sec, cm_to_counts,
    torque_nm_to_raw, current_raw_to_a,
    counts_to_cm, inc_per_sec_to_cm_s, inc_per_sec_to_rpm,
    kg_to_motor_torque_nm,
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
        self._pp_setpoint_pending = False
        self._pp_setpoint_pulse = 0
        self._pp_setpoint_delay = 0
        self._pp_wait_ack = False
        self._pp_ack_timeout = 0
        self._last_target_position = None
        self._last_torque_slope = None
        self._last_velocity_limit_raw = None
        self._last_mode = None
        self._reinitializing = False
        self._last_heartbeat_state = None
        self._heartbeat_timeout_counter = 0
        self._heartbeat_timeout_limit = 300  # 300 * 5ms = 1.5 seconds
        self._last_fault_state = False

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

    def _initialize_node(self):
        """Initialize node (call after connection or after boot-up detected)"""
        if not self.ser:
            return
        try:
            # Heartbeat producer time: 1000 ms
            sdo_write_u16(self.ser, 0x1017, 0x00, 1000)

            # Pre-Op -> Operational (NMT state machine)
            nmt_send(self.ser, 0x80, NODE_ID)
            QtCore.QThread.msleep(200)
            nmt_send(self.ser, 0x01, NODE_ID)
            QtCore.QThread.msleep(200)

            # CRITICAL: Start TX timer IMMEDIATELY after going operational
            # Must be done BEFORE enabling watchdog
            self._ensure_timers()
            if self._tx_timer and not self._tx_timer.isActive():
                self._tx_timer.setInterval(self.tx_interval_ms)
                self._tx_timer.start()

            # Send immediate RPDO to start communication
            self._send_cyclic()
            QtCore.QThread.msleep(20)

            # Enable/Disable Watchdog in Operational mode based on config setting
            # Sub-indices 01h and 02h control enable/status
            # Configuration (03h, 04h, 05h) is already set in Luna
            if WATCHDOG_ENABLED:
                sdo_write_u16(self.ser, 0x2060, 0x01, 0x0001)  # Enable watchdog
                QtCore.QThread.msleep(50)
                sdo_write_u16(self.ser, 0x2060, 0x02, 0x0001)  # Set watchdog status to active
                QtCore.QThread.msleep(50)
                self.status.emit("Watchdog enabled")
            else:
                sdo_write_u16(self.ser, 0x2060, 0x01, 0x0000)  # Disable watchdog
                QtCore.QThread.msleep(50)
                sdo_write_u16(self.ser, 0x2060, 0x02, 0x0000)  # Set watchdog status to inactive
                QtCore.QThread.msleep(50)
                self.status.emit("Watchdog disabled")

            cmd = self._state.snapshot().command
            sdo_write_u8(self.ser, 0x6060, 0x00, int(cmd.mode))
            QtCore.QThread.msleep(20)  # Allow TX timer to send RPDO

            # Profile position defaults
            sdo_write_u32(self.ser, 0x6083, 0x00, PROFILE_ACCEL_DEFAULT)
            sdo_write_u32(self.ser, 0x6084, 0x00, PROFILE_DECEL_DEFAULT)
            QtCore.QThread.msleep(20)  # Allow TX timer to send RPDO

            sdo_write_u32(self.ser, 0x6085, 0x00, QUICK_STOP_DECEL_DEFAULT)
            sdo_write_u32(self.ser, 0x607F, 0x00, MAX_PROFILE_VELOCITY_DEFAULT)
            QtCore.QThread.msleep(20)  # Allow TX timer to send RPDO

            velocity_limit_raw = rpm_to_raw(cmd.velocity_limit_rpm)
            rpdo2_send(
                self.ser,
                torque_slope=cmd.torque_slope,
                velocity_limit=velocity_limit_raw
            )
            self._last_torque_slope = cmd.torque_slope
            self._last_velocity_limit_raw = velocity_limit_raw
            self._last_mode = cmd.mode
        except (serial.SerialException, OSError, ValueError) as e:
            self.status.emit(f"Initialization error: {e}")

    @QtCore.Slot()
    def connect_bus(self):
        try:
            # Close any existing connection first
            if self.ser:
                try:
                    self.ser.close()
                except (serial.SerialException, OSError):
                    pass
                self.ser = None
                QtCore.QThread.msleep(100)  # Wait for port to fully close

            self.ser = serial.Serial(COM_PORT, SERIAL_BAUD, timeout=0.05)
            QtCore.QThread.msleep(50)  # Wait for port to stabilize

            # Clear any garbage in buffer
            self.ser.reset_input_buffer()
            self.ser.reset_output_buffer()
            self._rx_buf.clear()

            cfg = build_config_cmd_fixed20(
                can_bitrate_code=CAN_BITRATE_CODE,
                frame_type=0x01,
                filter_id=0x00000000,
                mask_id=0x00000000,
                can_mode=0x00,
                auto_retx=0x00
            )
            self.ser.write(cfg)
            self.ser.flush()
            QtCore.QThread.msleep(200)  # Wait for adapter to process config

            # NMT Reset Communication (0x82) - ensures clean communication start
            nmt_send(self.ser, 0x82, NODE_ID)
            QtCore.QThread.msleep(200)

            self._initialize_node()

            # TX timer is already started in _initialize_node()

            self.running = False  # Explicitly set to false after connect
            self._state.update_flags(running=False)
            self.status.emit("Motor stopped for safety")

            # Reset DC Bus Voltage max
            self._state.update_feedback(dc_bus_voltage_max=0.0)

            # Reset heartbeat timeout counter
            self._heartbeat_timeout_counter = 0
            self._last_fault_state = False

            # Ensure poll timer is running
            self._ensure_timers()
            if self._timer and not self._timer.isActive():
                self._timer.start()
                self.status.emit("Polling timer started")

            self._state.update_flags(connected=True)
            self.status.emit("Connected")
        except (serial.SerialException, OSError, ValueError) as e:
            self.ser = None
            self.status.emit(f"Connect error: {e}")

    def _emergency_disconnect(self):
        """Force emergency disconnect without sending commands (used when connection is broken)"""
        try:
            if self.ser:
                try:
                    self.ser.reset_input_buffer()
                    self.ser.reset_output_buffer()
                    self._rx_buf.clear()
                except (serial.SerialException, OSError):
                    pass
                try:
                    self.ser.close()
                except (serial.SerialException, OSError):
                    pass
        finally:
            self.ser = None
            self.running = False
            self._state.update_flags(connected=False, running=False)

    @QtCore.Slot()
    def disconnect_bus(self):
        # Stop TX timer (but keep poll timer running for reconnection)
        if self._tx_timer and self._tx_timer.isActive():
            self._tx_timer.stop()

        if self.ser:
            try:
                # Try to send shutdown sequence, but don't fail if connection is broken
                cmd = self._state.snapshot().command
                rpdo3_send(self.ser, target_velocity=0, target_position=0)
                rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
                QtCore.QThread.msleep(20)
                rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, cmd.mode, target_torque=0)
                QtCore.QThread.msleep(150)
            except (serial.SerialException, OSError):
                # Connection already broken, that's ok
                pass

            try:
                # Clear buffers before closing
                self.ser.reset_input_buffer()
                self.ser.reset_output_buffer()
                self._rx_buf.clear()
                self.ser.close()
            except (serial.SerialException, OSError):
                pass
            finally:
                self.ser = None

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

        # Proper CiA402 startup sequence with adequate delays
        # The drive needs time to process each state transition
        try:
            self.status.emit("Starting motor...")

            # State 1: Shutdown -> Ready to Switch On
            rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
            QtCore.QThread.msleep(30)

            # State 2: Shutdown -> Ready to Switch On
            rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
            QtCore.QThread.msleep(30)

            # State 3: Switch On Disabled -> Ready to Switch On
            rpdo1_send(self.ser, CW_SWITCH_ON, cmd.mode, target_torque=0)
            QtCore.QThread.msleep(30)

            # State 4: Ready to Switch On -> Switched On
            rpdo1_send(self.ser, CW_SWITCH_ON, cmd.mode, target_torque=0)
            QtCore.QThread.msleep(30)

            # State 5: Switched On -> Operation Enabled
            rpdo1_send(self.ser, CW_ENABLE_OPERATION, cmd.mode, target_torque=0)
            QtCore.QThread.msleep(50)

            # TX timer is already running, just update interval for cyclic sending
            self.tx_interval_ms = cmd.cycle_time_ms
            if self._tx_timer:
                self._tx_timer.setInterval(self.tx_interval_ms)

            self.running = True
            self._state.update_flags(running=True)
            self.status.emit("Started")
        except (serial.SerialException, OSError) as e:
            self.status.emit(f"Start error: {e}")

    @QtCore.Slot()
    def stop_motor(self):
        if not self.ser:
            return
        self._send_disable_sequence()
        self.running = False
        self._state.update_flags(running=False)
        self.status.emit("Stopped")

    @QtCore.Slot()
    def fault_reset(self):
        if not self.ser:
            self.status.emit("Not connected")
            return

        # Important: Stop motor and clear running flag before resetting fault
        # This prevents the motor from running away after fault reset
        if self.running:
            self.running = False
            self._state.update_flags(running=False)

        cw = CW_SHUTDOWN | 0x0080
        cmd = self._state.snapshot().command
        rpdo1_send(self.ser, cw, cmd.mode, target_torque=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
        QtCore.QThread.msleep(20)

        # NMT Reset Communication (0x82) - resets communication and may clear watchdog alarm
        try:
            nmt_send(self.ser, 0x82, NODE_ID)
            QtCore.QThread.msleep(200)
            # After reset communication, re-initialize the node
            self._initialize_node()
        except (serial.SerialException, OSError, ValueError) as e:
            self.status.emit(f"NMT reset error: {e}")

        self._state.update_flags(fault=False)
        self.status.emit("Fault reset - Motor stopped for safety. Press Start to resume.")

    def _sync_cycle_time(self, cycle_time_ms: int):
        cycle_time_ms = max(1, int(cycle_time_ms))
        if cycle_time_ms != self.tx_interval_ms:
            self.tx_interval_ms = cycle_time_ms
            if self._tx_timer:
                self._tx_timer.setInterval(self.tx_interval_ms)

    def _send_disable_sequence(self):
        # Proper shutdown sequence: Shutdown -> Disable Voltage
        cmd = self._state.snapshot().command
        rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
        rpdo3_send(self.ser, target_velocity=0, target_position=0)
        QtCore.QThread.msleep(20)
        rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, cmd.mode, target_torque=0)

    def _send_cyclic(self):
        if not self.ser:
            return

        snapshot = self._state.snapshot()
        cmd = snapshot.command

        if self.running:
            # Motor running: send full control commands
            self._sync_cycle_time(cmd.cycle_time_ms)

            if self._last_mode != cmd.mode:
                sdo_write_u8(self.ser, 0x6060, 0x00, int(cmd.mode))
                self._last_mode = cmd.mode

            direction = 1 if int(cmd.direction) >= 0 else -1
            target_velocity = rpm_to_raw(cmd.target_velocity_rpm) * direction
            # Convert kg to motor torque in Nm
            torque_nm = kg_to_motor_torque_nm(float(cmd.target_torque_mnm) / 1000.0)
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
                rpdo3_send(
                    self.ser,
                    target_velocity=target_velocity,
                    target_position=target_position
                )
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
                rpdo3_send(
                    self.ser,
                    target_velocity=target_velocity,
                    target_position=target_position
                )
                rpdo4_send(self.ser, profile_velocity=profile_velocity)
            else:
                rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=target_torque)
                rpdo3_send(
                    self.ser,
                    target_velocity=target_velocity,
                    target_position=target_position
                )
        else:
            # Motor not running: send safe state RPDOs (CW_SHUTDOWN, zero targets)
            # This keeps the drive responsive and prevents it from auto-stopping
            cmd = self._state.snapshot().command
            try:
                rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
                rpdo3_send(self.ser, target_velocity=0, target_position=0)
            except (serial.SerialException, OSError):
                pass

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

        # Heartbeat timeout monitoring
        self._heartbeat_timeout_counter += 1
        if self._heartbeat_timeout_counter > self._heartbeat_timeout_limit:
            if flags.connected:
                self.status.emit("ERROR: CAN communication lost! Disconnecting for safety.")
                # Use emergency disconnect (no commands, just cleanup)
                self._emergency_disconnect()
                return

        try:
            pending = self.ser.in_waiting
        except (serial.SerialException, OSError):
            # Connection lost, do emergency disconnect
            if flags.connected:
                self.status.emit("ERROR: Serial connection lost! Disconnecting for safety.")
                self._emergency_disconnect()
            return
        if pending:
            try:
                chunk = self.ser.read(pending)
            except (serial.SerialException, OSError):
                # Connection lost during read
                if flags.connected:
                    self.status.emit("ERROR: Serial read error! Disconnecting for safety.")
                    self._emergency_disconnect()
                return
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

                    # Reset heartbeat timeout counter on valid heartbeat
                    self._heartbeat_timeout_counter = 0

                    # Handle unexpected non-operational states:
                    # 0x00 = Boot-up, 0x7F = Pre-Operational
                    # Expected: 0x05 = Operational
                    flags = self._state.snapshot().flags

                    # Detect if controller needs re-initialization
                    needs_reinit = False
                    if state == 0x00:
                        # Boot-up detected
                        needs_reinit = True
                    elif state == 0x7F and flags.connected:
                        # Pre-Operational when we expect Operational
                        # Only reinit if we had a previous operational state
                        if self._last_heartbeat_state == 0x05 or self._last_heartbeat_state is None:
                            needs_reinit = True

                    if needs_reinit and not self._reinitializing and flags.connected:
                        self._reinitializing = True
                        if state == 0x00:
                            self.status.emit("Controller boot-up detected - initializing...")
                        else:
                            self.status.emit("Controller in Pre-Op - switching to Operational...")

                        QtCore.QThread.msleep(100)
                        self._initialize_node()

                        # Restart motor if it was running
                        if flags.running:
                            QtCore.QThread.msleep(100)
                            cmd = self._state.snapshot().command
                            rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0)
                            QtCore.QThread.msleep(20)
                            rpdo1_send(self.ser, CW_SWITCH_ON, cmd.mode, target_torque=0)
                            QtCore.QThread.msleep(20)
                            rpdo1_send(self.ser, CW_ENABLE_OPERATION, cmd.mode, target_torque=0)
                            self.status.emit("Motor restarted - now Operational")
                        else:
                            self.status.emit("Controller switched to Operational")
                        self._reinitializing = False

                    self._last_heartbeat_state = state
            elif 0x180 <= can_id <= 0x1FF and dlc >= 5:
                node_id = can_id - 0x180
                if node_id == NODE_ID:
                    # Reset heartbeat timeout on TPDO1
                    self._heartbeat_timeout_counter = 0

                    statusword, error_code, mode_disp = struct.unpack_from("<HHB", data, 0)
                    if statusword & 0x1000:
                        self._pp_wait_ack = False
                    fault = bool(statusword & (1 << 3))
                    target_reached = bool(statusword & (1 << 10))

                    # Safety: Auto-stop on fault detection
                    if fault and not self._last_fault_state:
                        # Fault just occurred
                        if self.running:
                            self.running = False
                            self._state.update_flags(running=False)
                            self.status.emit(
                                f"FAULT DETECTED (Error: 0x{error_code:04X})! "
                                f"Motor stopped for safety."
                            )

                    self._last_fault_state = fault

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
                    # Reset heartbeat timeout on TPDO2
                    self._heartbeat_timeout_counter = 0

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
                    # Reset heartbeat timeout on TPDO3
                    self._heartbeat_timeout_counter = 0

                    # Current actual value (0x6078) at bytes 0-1
                    current = struct.unpack_from("<h", data, 0)[0]
                    dc_bus_voltage = 0.0
                    drive_temp = 0
                    chassis_temp = 0

                    # TPDO3 format:
                    # Bytes 0-1: Current
                    # Bytes 2-3: DC Bus Voltage (0x2030)
                    # Bytes 4-5: Chassis Temperature (0x2019:02)
                    # Bytes 6-7: Drive Temperature (0x2019:01)
                    if dlc >= 8:
                        dc_bus_raw = struct.unpack_from("<H", data, 2)[0]
                        dc_bus_voltage = dc_bus_raw / 10.0  # Convert to V
                        chassis_temp_raw = struct.unpack_from("<h", data, 4)[0]
                        drive_temp_raw = struct.unpack_from("<h", data, 6)[0]
                        # Convert from 0.1Â°C to Â°C
                        chassis_temp = chassis_temp_raw / 10.0
                        drive_temp = drive_temp_raw / 10.0

                    # Calculate torque from current
                    current_a = current_raw_to_a(current)
                    torque_nm = abs((current_a / RATED_CURRENT_A) * RATED_TORQUE_NM)

                    # Update max DC Bus Voltage
                    snapshot = self._state.snapshot()
                    dc_bus_voltage_max = max(snapshot.feedback.dc_bus_voltage_max, dc_bus_voltage)

                    self._state.update_feedback(
                        current_a=current_a,
                        torque_mnm=torque_nm * 1000.0,
                        dc_bus_voltage=dc_bus_voltage,
                        dc_bus_voltage_max=dc_bus_voltage_max,
                        drive_temperature=drive_temp,
                        chassis_temperature=chassis_temp,
                        last_tpdo=can_id
                    )
