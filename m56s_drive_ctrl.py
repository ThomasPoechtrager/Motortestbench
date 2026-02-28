import struct
import serial
from PySide6 import QtCore

from config import (
    COM_PORT, SERIAL_BAUD, CAN_BITRATE_CODE, NODE_ID,
    COB_NMT, COB_SDO_RX_BASE, COB_RPDO1_BASE, COB_RPDO2_BASE, COB_RPDO3_BASE, COB_RPDO4_BASE,
    CW_SHUTDOWN, CW_DISABLE_VOLTAGE,
    MODE_POSITION,
    PROFILE_ACCEL_DEFAULT, PROFILE_DECEL_DEFAULT, QUICK_STOP_DECEL_DEFAULT,
    MAX_PROFILE_VELOCITY_DEFAULT,
    POLL_INTERVAL_MS, TX_INTERVAL_MS, WATCHDOG_ENABLED,
    HEARTBEAT_PRODUCER_MS, HEARTBEAT_TIMEOUT_MS,
    COMM_TIMEOUT_ERROR_CODE,
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


def sdo_write_u8(ser, index, subindex, value, node_id):
    data = bytes([
        0x2F,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        value & 0xFF,
        0x00, 0x00, 0x00
    ])
    send_can(ser, COB_SDO_RX_BASE + node_id, data)


def sdo_write_u16(ser, index, subindex, value, node_id):
    data = bytes([
        0x2B,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        value & 0xFF, (value >> 8) & 0xFF,
        0x00, 0x00
    ])
    send_can(ser, COB_SDO_RX_BASE + node_id, data)


def sdo_write_u32(ser, index, subindex, value, node_id):
    data = bytes([
        0x23,
        index & 0xFF, (index >> 8) & 0xFF,
        subindex,
        value & 0xFF, (value >> 8) & 0xFF,
        (value >> 16) & 0xFF, (value >> 24) & 0xFF
    ])
    send_can(ser, COB_SDO_RX_BASE + node_id, data)


def nmt_send(ser, command, node_id):
    send_can(ser, COB_NMT, bytes([command, node_id]))





def rpdo1_send(ser, controlword, mode, target_torque, node_id=NODE_ID):
    data = bytearray(8)
    data[0:2] = int(controlword).to_bytes(2, "little", signed=False)
    data[2] = int(mode) & 0xFF
    data[3:5] = int(target_torque).to_bytes(2, "little", signed=True)
    data[5] = 0x00
    send_can(ser, COB_RPDO1_BASE + node_id, bytes(data[:5]))


def rpdo2_send(ser, torque_slope, velocity_limit, node_id=NODE_ID):
    data = bytearray(8)
    data[0:4] = int(torque_slope).to_bytes(4, "little", signed=True)
    data[4:8] = int(velocity_limit).to_bytes(4, "little", signed=True)
    send_can(ser, COB_RPDO2_BASE + node_id, bytes(data))


def rpdo3_send(ser, target_velocity, target_position, node_id=NODE_ID):
    data = bytearray(8)
    data[0:4] = int(target_velocity).to_bytes(4, "little", signed=True)
    data[4:8] = int(target_position).to_bytes(4, "little", signed=True)
    send_can(ser, COB_RPDO3_BASE + node_id, bytes(data))


def rpdo4_send(ser, profile_velocity, node_id=NODE_ID):
    data = int(profile_velocity).to_bytes(4, "little", signed=False)
    send_can(ser, COB_RPDO4_BASE + node_id, data)


class DriveController(QtCore.QObject):
    status = QtCore.Signal(str)

    def __init__(self, drive_states: dict[int, DriveState]):
        super().__init__()
        self._drive_states = drive_states
        self._node_ids = sorted(drive_states.keys())
        self.ser = None
        self.tx_interval_ms = TX_INTERVAL_MS
        self._timer = None
        self._tx_timer = None
        self._rx_buf = bytearray()
        self._heartbeat_timeout_limit = max(1, int(HEARTBEAT_TIMEOUT_MS / POLL_INTERVAL_MS))

        self._runtime = {
            node_id: {
                "reinitializing": False,
                "last_heartbeat_state": None,
                "heartbeat_timeout_counter": 0,
                "last_fault_state": False,
                "last_torque_slope": None,
                "last_velocity_limit_raw": None,
                "last_mode": None,
            }
            for node_id in self._node_ids
        }

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
        self._ensure_timers()
        if self._timer and not self._timer.isActive():
            self._timer.start()
            self.status.emit("Polling started")

    @QtCore.Slot()
    def shutdown(self):
        if self._tx_timer and self._tx_timer.isActive():
            self._tx_timer.stop()
        if self._timer and self._timer.isActive():
            self._timer.stop()
        if self._tx_timer is not None:
            self._tx_timer.deleteLater()
            self._tx_timer = None
        if self._timer is not None:
            self._timer.deleteLater()
            self._timer = None
        self._emergency_disconnect_all()

    def _service_connected_nodes(self, exclude_node_id: int | None = None):
        if not self.ser:
            return
        for other_node_id in self._node_ids:
            if exclude_node_id is not None and other_node_id == exclude_node_id:
                continue
            if self._drive_states[other_node_id].snapshot().flags.connected:
                self._send_cyclic_for_node(other_node_id)
        try:
            pending = self.ser.in_waiting
        except (serial.SerialException, OSError):
            return
        if pending:
            try:
                chunk = self.ser.read(pending)
            except (serial.SerialException, OSError):
                return
            if chunk:
                self._rx_buf.extend(chunk)
                self._parse_rx_frames()

    def _sleep_with_service(self, total_ms: int, exclude_node_id: int | None = None):
        remaining = max(0, int(total_ms))
        while remaining > 0:
            step_ms = 10 if remaining >= 10 else remaining
            QtCore.QThread.msleep(step_ms)
            self._service_connected_nodes(exclude_node_id=exclude_node_id)
            remaining -= step_ms

    def _open_bus(self):
        if self.ser:
            return
        self.ser = serial.Serial(COM_PORT, SERIAL_BAUD, timeout=0.05)
        QtCore.QThread.msleep(50)
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
        QtCore.QThread.msleep(200)

        self._ensure_timers()
        if self._tx_timer and not self._tx_timer.isActive():
            self._tx_timer.setInterval(self.tx_interval_ms)
            self._tx_timer.start()

        self.status.emit(f"Bus connected on {COM_PORT}")

    def _close_bus(self):
        if self._tx_timer and self._tx_timer.isActive():
            self._tx_timer.stop()
        if self.ser:
            try:
                self.ser.reset_input_buffer()
                self.ser.reset_output_buffer()
                self._rx_buf.clear()
                self.ser.close()
            except (serial.SerialException, OSError):
                pass
            finally:
                self.ser = None

    def _initialize_node(self, node_id: int):
        if not self.ser:
            return
        state = self._drive_states[node_id]
        runtime = self._runtime[node_id]
        try:
            sdo_write_u16(self.ser, 0x1017, 0x00, HEARTBEAT_PRODUCER_MS, node_id)
            nmt_send(self.ser, 0x80, node_id)
            self._sleep_with_service(150, exclude_node_id=node_id)
            nmt_send(self.ser, 0x01, node_id)
            self._sleep_with_service(150, exclude_node_id=node_id)

            cmd = state.snapshot().command
            sdo_write_u8(self.ser, 0x6060, 0x00, int(cmd.mode), node_id)
            self._sleep_with_service(10, exclude_node_id=node_id)
            sdo_write_u32(self.ser, 0x6083, 0x00, PROFILE_ACCEL_DEFAULT, node_id)
            sdo_write_u32(self.ser, 0x6084, 0x00, PROFILE_DECEL_DEFAULT, node_id)
            self._sleep_with_service(10, exclude_node_id=node_id)
            sdo_write_u32(self.ser, 0x6085, 0x00, QUICK_STOP_DECEL_DEFAULT, node_id)
            sdo_write_u32(self.ser, 0x607F, 0x00, MAX_PROFILE_VELOCITY_DEFAULT, node_id)
            self._sleep_with_service(10, exclude_node_id=node_id)

            if WATCHDOG_ENABLED:
                sdo_write_u16(self.ser, 0x2060, 0x01, 0x0001, node_id)
                self._sleep_with_service(20, exclude_node_id=node_id)
                sdo_write_u16(self.ser, 0x2060, 0x02, 0x0001, node_id)
                self._sleep_with_service(20, exclude_node_id=node_id)
            else:
                sdo_write_u16(self.ser, 0x2060, 0x01, 0x0000, node_id)
                self._sleep_with_service(20, exclude_node_id=node_id)
                sdo_write_u16(self.ser, 0x2060, 0x02, 0x0000, node_id)
                self._sleep_with_service(20, exclude_node_id=node_id)

            velocity_limit_raw = rpm_to_raw(cmd.velocity_limit_rpm)
            rpdo2_send(self.ser, cmd.torque_slope, velocity_limit_raw, node_id=node_id)
            runtime["last_torque_slope"] = cmd.torque_slope
            runtime["last_velocity_limit_raw"] = velocity_limit_raw
            runtime["last_mode"] = cmd.mode
            runtime["heartbeat_timeout_counter"] = 0
            runtime["last_fault_state"] = False

            state.update_feedback(dc_bus_voltage_max=0.0, error_code=0)
            state.update_flags(connected=True, running=False)
            self.status.emit(f"Node {node_id}: connected")
        except (serial.SerialException, OSError, ValueError) as e:
            self.status.emit(f"Node {node_id}: initialization error: {e}")

    def _emergency_disconnect_all(self):
        self._close_bus()
        for _, state in self._drive_states.items():
            state.update_flags(connected=False, running=False)

    def _send_disable_sequence(self, node_id: int):
        if not self.ser:
            return
        cmd = self._drive_states[node_id].snapshot().command
        rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0, node_id=node_id)
        rpdo3_send(self.ser, target_velocity=0, target_position=0, node_id=node_id)
        self._sleep_with_service(10, exclude_node_id=node_id)
        rpdo1_send(self.ser, CW_DISABLE_VOLTAGE, cmd.mode, target_torque=0, node_id=node_id)
        self._runtime[node_id]["last_controlword"] = CW_DISABLE_VOLTAGE

    def _fault_reset(self, node_id: int):
        if not self.ser:
            self.status.emit(f"Node {node_id}: bus not connected")
            return
        state = self._drive_states[node_id]
        cmd = state.snapshot().command
        try:
            cw = CW_SHUTDOWN | 0x0080
            rpdo1_send(self.ser, cw, cmd.mode, target_torque=0, node_id=node_id)
            self._sleep_with_service(20, exclude_node_id=node_id)
            rpdo1_send(self.ser, CW_SHUTDOWN, cmd.mode, target_torque=0, node_id=node_id)
            self._sleep_with_service(20, exclude_node_id=node_id)
            nmt_send(self.ser, 0x82, node_id)
            self._sleep_with_service(150, exclude_node_id=node_id)
            self._initialize_node(node_id)
            state.update_flags(fault=False)
            self.status.emit(f"Node {node_id}: fault reset")
        except (serial.SerialException, OSError, ValueError) as e:
            self.status.emit(f"Node {node_id}: fault reset error: {e}")

    def _sync_cycle_time(self):
        cycle_time = self.tx_interval_ms
        for state in self._drive_states.values():
            cycle_time = min(cycle_time, max(1, int(state.snapshot().command.cycle_time_ms)))
        if cycle_time != self.tx_interval_ms:
            self.tx_interval_ms = cycle_time
            if self._tx_timer:
                self._tx_timer.setInterval(self.tx_interval_ms)

    def _send_cyclic_for_node(self, node_id: int):
        """Send cyclic RPDOs - controlword managed by Control layer"""
        if not self.ser:
            return
        state = self._drive_states[node_id]
        snapshot = state.snapshot()
        cmd = snapshot.command
        runtime = self._runtime[node_id]

        # Update mode if changed
        if runtime["last_mode"] != cmd.mode:
            sdo_write_u8(self.ser, 0x6060, 0x00, int(cmd.mode), node_id)
            runtime["last_mode"] = cmd.mode

        # Update torque slope & velocity limit if changed
        velocity_limit_raw = rpm_to_raw(cmd.velocity_limit_rpm)
        if (
            cmd.torque_slope != runtime["last_torque_slope"]
            or velocity_limit_raw != runtime["last_velocity_limit_raw"]
        ):
            rpdo2_send(self.ser, cmd.torque_slope, velocity_limit_raw, node_id=node_id)
            runtime["last_torque_slope"] = cmd.torque_slope
            runtime["last_velocity_limit_raw"] = velocity_limit_raw

        # Prepare setpoint values
        direction = 1 if int(cmd.direction) >= 0 else -1
        target_velocity = rpm_to_raw(cmd.target_velocity_rpm) * direction
        torque_nm = kg_to_motor_torque_nm(float(cmd.target_torque_mnm) / 1000.0)
        target_torque = torque_nm_to_raw(torque_nm) * direction
        target_position = cm_to_counts(cmd.target_position_cm)
        profile_velocity = rpm_to_pulses_per_sec(cmd.profile_velocity_rpm)

        # Send controlword from Control layer (RPDO1)
        try:
            rpdo1_send(self.ser, cmd.controlword, cmd.mode, target_torque=target_torque, node_id=node_id)
            
            # Send setpoints (RPDO3)
            rpdo3_send(self.ser, target_velocity=target_velocity, target_position=target_position, node_id=node_id)
            
            # Send profile velocity for position mode (RPDO4)
            if cmd.mode == MODE_POSITION:
                rpdo4_send(self.ser, profile_velocity=profile_velocity, node_id=node_id)
        except (serial.SerialException, OSError):
            pass

    def _send_cyclic(self):
        if not self.ser:
            return
        self._sync_cycle_time()
        for node_id in sorted(self._node_ids):
            if self._drive_states[node_id].snapshot().flags.connected:
                self._send_cyclic_for_node(node_id)

    def _parse_rx_frames(self):
        while True:
            idx_aa55 = self._rx_buf.find(b"\xAA\x55")
            idx_55aa = self._rx_buf.find(b"\x55\xAA")
            if idx_aa55 < 0 and idx_55aa < 0:
                if len(self._rx_buf) > 64:
                    self._rx_buf.clear()
                break
            idx = min([i for i in (idx_aa55, idx_55aa) if i >= 0], default=-1)
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
                data = bytes(self._rx_buf[5:5 + dlc])
                del self._rx_buf[:frame_len]
            else:
                if len(self._rx_buf) < 20:
                    break
                frame = bytes(self._rx_buf[:20])
                del self._rx_buf[:20]
                can_id = int.from_bytes(frame[5:9], byteorder="little", signed=False)
                dlc = frame[9] & 0x0F
                data = frame[10:10 + dlc]

            self._handle_can_frame(can_id, dlc, data)

    def _handle_can_frame(self, can_id: int, dlc: int, data: bytes):
        if 0x700 <= can_id <= 0x77F and dlc >= 1:
            node_id = can_id - 0x700  # node_id extraction from heartbeat COB-ID
            if node_id in self._drive_states:
                self._runtime[node_id]["heartbeat_timeout_counter"] = 0
                hb_state = data[0]
                flags = self._drive_states[node_id].snapshot().flags
                needs_reinit = (
                    hb_state in (0x00, 0x7F)
                    and flags.connected
                    and not self._runtime[node_id]["reinitializing"]
                )
                if needs_reinit:
                    self._runtime[node_id]["reinitializing"] = True
                    self.status.emit(f"Node {node_id}: reinitializing from HB state 0x{hb_state:02X}")
                    self._sleep_with_service(80, exclude_node_id=node_id)
                    self._initialize_node(node_id)
                    self._runtime[node_id]["reinitializing"] = False
                self._runtime[node_id]["last_heartbeat_state"] = hb_state
            return

        if 0x180 <= can_id <= 0x1FF and dlc >= 5:
            node_id = can_id - 0x180  # node_id extraction from TPDO1 COB-ID
            if node_id in self._drive_states:
                state = self._drive_states[node_id]
                runtime = self._runtime[node_id]
                runtime["heartbeat_timeout_counter"] = 0
                statusword, error_code, mode_disp = struct.unpack_from("<HHB", data, 0)
                fault = bool(statusword & (1 << 3))
                target_reached = bool(statusword & (1 << 10))
                if fault and not runtime["last_fault_state"]:
                    # Only report fault, let Control layer manage running state
                    self.status.emit(f"Node {node_id}: FAULT 0x{error_code:04X}")
                runtime["last_fault_state"] = fault
                state.update_feedback(
                    statusword=statusword,
                    error_code=error_code,
                    mode_display=mode_disp,
                    last_tpdo=can_id,
                )
                state.update_flags(fault=fault, target_reached=target_reached)
            return

        if 0x280 <= can_id <= 0x2FF and dlc >= 8:
            node_id = can_id - 0x280  # node_id extraction from TPDO2 COB-ID
            if node_id in self._drive_states:
                state = self._drive_states[node_id]
                self._runtime[node_id]["heartbeat_timeout_counter"] = 0
                position, velocity = struct.unpack_from("<ii", data, 0)
                state.update_feedback(
                    position_cm=counts_to_cm(position),
                    speed_cm_s=inc_per_sec_to_cm_s(velocity),
                    speed_rpm=inc_per_sec_to_rpm(velocity),
                    last_tpdo=can_id,
                )
            return

        if 0x380 <= can_id <= 0x3FF and dlc >= 2:
            node_id = can_id - 0x380  # node_id extraction from TPDO3 COB-ID
            if node_id in self._drive_states:
                state = self._drive_states[node_id]
                self._runtime[node_id]["heartbeat_timeout_counter"] = 0
                current = struct.unpack_from("<h", data, 0)[0]
                dc_bus_voltage = 0.0
                drive_temp = 0.0
                chassis_temp = 0.0
                if dlc >= 8:
                    dc_bus_raw = struct.unpack_from("<H", data, 2)[0]
                    dc_bus_voltage = dc_bus_raw / 10.0
                    chassis_temp = struct.unpack_from("<h", data, 4)[0] / 10.0
                    drive_temp = struct.unpack_from("<h", data, 6)[0] / 10.0
                current_a = current_raw_to_a(current)
                torque_nm = abs((current_a / RATED_CURRENT_A) * RATED_TORQUE_NM)
                snapshot = state.snapshot()
                dc_bus_voltage_max = max(snapshot.feedback.dc_bus_voltage_max, dc_bus_voltage)
                state.update_feedback(
                    current_a=current_a,
                    torque_mnm=torque_nm * 1000.0,
                    dc_bus_voltage=dc_bus_voltage,
                    dc_bus_voltage_max=dc_bus_voltage_max,
                    drive_temperature=drive_temp,
                    chassis_temperature=chassis_temp,
                    last_tpdo=can_id,
                )

    def _poll(self):
        for node_id, state in self._drive_states.items():
            flags = state.snapshot().flags

            if flags.request_connect:
                try:
                    self._open_bus()
                    if self.ser:
                        nmt_send(self.ser, 0x82, node_id)
                        self._sleep_with_service(120, exclude_node_id=node_id)
                        self._initialize_node(node_id)
                except (serial.SerialException, OSError, ValueError) as e:
                    self.status.emit(f"Node {node_id}: connect error: {e}")
                    state.update_flags(connected=False)  # Let Control layer manage running
                finally:
                    state.update_flags(request_connect=False)

            if flags.request_disconnect:
                if self.ser and state.snapshot().flags.connected:
                    try:
                        self._send_disable_sequence(node_id)
                    except (serial.SerialException, OSError):
                        pass
                state.update_flags(connected=False, running=False, request_disconnect=False)
                self.status.emit(f"Node {node_id}: disconnected")

            if flags.request_fault_reset:
                self._fault_reset(node_id)
                state.update_flags(request_fault_reset=False)

            # Note: request_start and request_stop are now handled by Control layer
            # which manages the controlword state machine

        connected_nodes = [
            node_id for node_id, state in self._drive_states.items() if state.snapshot().flags.connected
        ]
        if not connected_nodes:
            if self.ser:
                self._close_bus()
                self.status.emit("Bus disconnected")
            return

        if not self.ser:
            return

        for node_id in connected_nodes:
            runtime = self._runtime[node_id]
            runtime["heartbeat_timeout_counter"] += 1
            if runtime["heartbeat_timeout_counter"] > self._heartbeat_timeout_limit:
                self._drive_states[node_id].update_feedback(error_code=COMM_TIMEOUT_ERROR_CODE)
                # Disconnect on timeout, but let Control layer manage running state
                self._drive_states[node_id].update_flags(connected=False)
                self.status.emit(f"Node {node_id}: communication timeout")

        try:
            pending = self.ser.in_waiting
        except (serial.SerialException, OSError):
            for node_id in connected_nodes:
                self._drive_states[node_id].update_feedback(error_code=COMM_TIMEOUT_ERROR_CODE)
            self.status.emit("ERROR: Serial connection lost! Disconnecting bus.")
            self._emergency_disconnect_all()
            return

        if pending:
            try:
                chunk = self.ser.read(pending)
            except (serial.SerialException, OSError):
                for node_id in connected_nodes:
                    self._drive_states[node_id].update_feedback(error_code=COMM_TIMEOUT_ERROR_CODE)
                self.status.emit("ERROR: Serial read error! Disconnecting bus.")
                self._emergency_disconnect_all()
                return
            if chunk:
                self._rx_buf.extend(chunk)
                self._parse_rx_frames()
