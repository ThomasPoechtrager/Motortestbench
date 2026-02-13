import sys
from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg
import serial
import struct
import math

# Reuse existing CAN helpers/constants
from m56s_heartbeat import (
    COM_PORT, SERIAL_BAUD, CAN_BITRATE_CODE, NODE_ID,
    MODE_TORQUE, MODE_VELOCITY,
    CW_SHUTDOWN, CW_SWITCH_ON, CW_ENABLE_OPERATION, CW_HALT_BIT,
    build_config_cmd_fixed20,
    checksum_low8,
    sdo_write_u16, nmt_send,
    rpdo1_send, rpdo2_send, rpdo3_send
)

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

def rpm_to_raw(rpm: float) -> int:
    return int(round(rpm * RPM_TO_RAW))

def inc_per_sec_to_rpm(inc_per_sec: int) -> int:
    return int((inc_per_sec * 60) / INC_PER_REV)

def counts_to_cm(counts: int) -> float:
    return (counts / (INC_PER_REV * GEAR_RATIO)) * CM_PER_SHAFT_REV

def inc_per_sec_to_cm_s(inc_per_sec: int) -> float:
    return (inc_per_sec / (INC_PER_REV * GEAR_RATIO)) * CM_PER_SHAFT_REV

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
        self.target = 0  # raw units
        self.torque_slope = 5000
        self.velocity_limit = 10000
        self.direction = 1
        self.tx_interval_ms = 20
        self._timer = None
        self._tx_timer = None
        self._last_torque = None
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
                rpdo3_send(self.ser, target_velocity=0)
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
        self.status.emit(f"Mode set: {'Velocity' if mode == MODE_VELOCITY else 'Torque'}")

    @QtCore.Slot(int)
    def set_target(self, value):
        if self.mode == MODE_VELOCITY:
            self.target = rpm_to_raw(value) * self.direction
            self.status.emit(f"Target set: {value} rpm -> {self.target} raw")
        else:
            torque_nm = float(value) / TORQUE_INPUT_SCALE
            self.target = torque_nm_to_raw(torque_nm) * self.direction
            self.status.emit(f"Target set: {torque_nm:.3f} Nm -> {self.target} raw")

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

    def _send_disable_sequence(self):
        cw = CW_ENABLE_OPERATION | CW_HALT_BIT
        if self.mode == MODE_VELOCITY:
            rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
            rpdo3_send(self.ser, target_velocity=0)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=0)
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
            rpdo3_send(self.ser, target_velocity=self.target)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=self.target)

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

        self.target_label = QtWidgets.QLabel("Target (rpm)")
        self.target_spin = QtWidgets.QSpinBox()
        self.target_spin.setRange(-100000, 100000)
        self.target_spin.setValue(1000)
        self.target_spin.setKeyboardTracking(False)

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

        self.status = QtWidgets.QLabel("Disconnected")
        self.rx_label = QtWidgets.QLabel("RX frames: 0")
        self.rx_bytes_label = QtWidgets.QLabel("RX bytes: 0")
        self.frame_debug_label = QtWidgets.QLabel("Last TPDO: -")
        self.tpdo1_label = QtWidgets.QLabel("TPDO1: status=0x---- err=0x---- mode=--")
        self.tpdo2_label = QtWidgets.QLabel("TPDO2: pos=0 vel=0 rpm")
        self.tpdo3_label = QtWidgets.QLabel("TPDO3: current=0 torque=0")
        self.statusword_label = QtWidgets.QLabel("Statusword: 0x----")
        self.status_bits_group = self._build_status_bits()

        # Plot placeholder
        self.plot = pg.PlotWidget(title="Telemetry (placeholder)")
        self.plot.plot([0, 1, 2], [0, 0, 0])

        # Layout
        form = QtWidgets.QFormLayout()
        form.addRow("Mode", self.mode_combo)
        form.addRow(self.target_label, self.target_spin)
        form.addRow("Torque slope (0.1%/s)", self.torque_slope_spin)
        form.addRow("Velocity limit (motor rpm)", self.velocity_limit_spin)
        form.addRow("Direction", self.direction_combo)
        form.addRow("Cycle time (ms)", self.cycle_time_spin)

        btns = QtWidgets.QHBoxLayout()
        btns.addWidget(self.btn_connect)
        btns.addWidget(self.btn_disconnect)
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addLayout(btns)
        layout.addWidget(self.status)
        layout.addWidget(self.rx_label)
        layout.addWidget(self.rx_bytes_label)
        layout.addWidget(self.frame_debug_label)
        layout.addWidget(self.tpdo1_label)
        layout.addWidget(self.statusword_label)
        layout.addWidget(self.status_bits_group)
        layout.addWidget(self.tpdo2_label)
        layout.addWidget(self.tpdo3_label)
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
        self.mode_combo.currentIndexChanged.connect(self._mode_changed)
        self.target_spin.editingFinished.connect(self._target_committed)
        self.torque_slope_spin.editingFinished.connect(self._torque_slope_committed)
        self.velocity_limit_spin.editingFinished.connect(self._velocity_limit_committed)
        self.cycle_time_spin.editingFinished.connect(self._cycle_committed)
        self.direction_combo.currentIndexChanged.connect(self._direction_changed)
        self.worker.status.connect(self.status.setText)
        self.worker.tpdo1.connect(self._on_tpdo1)
        self.worker.tpdo2.connect(self._on_tpdo2)
        self.worker.tpdo3.connect(self._on_tpdo3)
        self.worker.frame_debug.connect(self._on_frame_debug)
        self.worker.raw_frame.connect(self._on_raw_frame)
        self.worker.raw_bytes.connect(self._on_raw_bytes)

        # Init
        self._mode_changed(0)
        self._target_committed()
        self._torque_slope_committed()
        self._velocity_limit_committed()
        self._cycle_committed()
        self.worker.set_direction(self.direction_combo.currentData())

    def _mode_changed(self, _):
        mode = self.mode_combo.currentData()
        self.worker.set_mode(mode)
        if mode == MODE_VELOCITY:
            self.target_label.setText("Target (rpm)")
        else:
            self.target_label.setText("Target (mNm)")
        self._target_committed()

    def _direction_changed(self, _):
        self.worker.set_direction(self.direction_combo.currentData())
        self._target_committed()

    def _target_committed(self):
        self.target_spin.interpretText()
        self.worker.set_target(self.target_spin.value())

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

    def _on_tpdo3(self, current: int, torque):
        current_a = current_raw_to_a(current)
        if torque is None:
            torque_nm = (current_a / RATED_CURRENT_A) * RATED_TORQUE_NM
            self.tpdo3_label.setText(
                f"TPDO3: current={current_a:.2f} A torque~{torque_nm:.2f} Nm"
            )
        else:
            torque_nm = torque_raw_to_nm(torque)
            self.tpdo3_label.setText(
                f"TPDO3: current={current_a:.2f} A torque={torque_nm:.2f} Nm"
            )

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
            grid.addWidget(led, row, 0)
            grid.addWidget(text, row, 1)
            self._status_leds[bit] = led

        grid.setColumnStretch(2, 1)
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