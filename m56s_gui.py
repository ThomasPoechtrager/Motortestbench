import sys
from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg
import serial

# Reuse existing CAN helpers/constants
from m56s_heartbeat import (
    COM_PORT, SERIAL_BAUD, CAN_BITRATE_CODE, NODE_ID,
    MODE_TORQUE, MODE_VELOCITY,
    CW_ENABLE_OPERATION, CW_HALT_BIT,
    build_config_cmd_fixed20,
    read_frame_fixed20, parse_fixed20,
    sdo_write_u16, nmt_send,
    enable_drive_sdo, disable_drive_sdo,
    rpdo1_send, rpdo2_send, rpdo3_send
)

RPM_TO_RAW = 10000 / 60

def rpm_to_raw(rpm: float) -> int:
    return int(round(rpm * RPM_TO_RAW))

class CanWorker(QtCore.QObject):
    status = QtCore.Signal(str)

    def __init__(self):
        super().__init__()
        self.ser = None
        self.running = False
        self.mode = MODE_VELOCITY
        self.target = 0  # raw units
        self.torque_slope = 5000
        self.velocity_limit = 10000
        self.direction = 1
        self._timer = QtCore.QTimer()
        self._timer.setInterval(50)
        self._timer.timeout.connect(self._poll)

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

            # Drive enable sequence (SDO)
            enable_drive_sdo(self.ser)

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
        self.stop_motor()
        if self.ser:
            try:
                self.ser.close()
            except Exception:
                pass
            self.ser = None
        self._timer.stop()
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
            self.target = int(value) * self.direction
            self.status.emit(f"Target set: {self.target}")

    @QtCore.Slot(int)
    def set_torque_slope(self, value):
        self.torque_slope = int(value)
        if self.ser:
            rpdo2_send(self.ser, torque_slope=self.torque_slope, velocity_limit=self.velocity_limit)
            self.status.emit(f"Torque slope set: {self.torque_slope}")

    @QtCore.Slot(int)
    def set_velocity_limit(self, value):
        self.velocity_limit = int(value)
        if self.ser:
            rpdo2_send(self.ser, torque_slope=self.torque_slope, velocity_limit=self.velocity_limit)
            self.status.emit(f"Velocity limit set: {self.velocity_limit}")

    @QtCore.Slot(int)
    def set_direction(self, value):
        self.direction = 1 if int(value) >= 0 else -1
        self.status.emit(f"Direction set: {'CW' if self.direction > 0 else 'CCW'}")

    @QtCore.Slot()
    def start_motor(self):
        if not self.ser:
            self.status.emit("Not connected")
            return
        cw = CW_ENABLE_OPERATION
        if self.mode == MODE_VELOCITY:
            rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
            QtCore.QThread.msleep(10)
            rpdo3_send(self.ser, target_velocity=self.target)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=self.target)
        self.running = True
        self.status.emit("Started")

    @QtCore.Slot()
    def stop_motor(self):
        if not self.ser:
            return
        cw = CW_ENABLE_OPERATION | CW_HALT_BIT
        if self.mode == MODE_VELOCITY:
            rpdo1_send(self.ser, cw, MODE_VELOCITY, target_torque=0)
            QtCore.QThread.msleep(10)
            rpdo3_send(self.ser, target_velocity=0)
        else:
            rpdo1_send(self.ser, cw, MODE_TORQUE, target_torque=0)
        QtCore.QThread.msleep(20)
        disable_drive_sdo(self.ser)
        self.running = False
        self.status.emit("Stopped")

    def _poll(self):
        if not self.ser:
            return
        fr = read_frame_fixed20(self.ser, timeout_s=0.0)
        if fr:
            can_id, _, _, dlc, data = parse_fixed20(fr)
            if 0x700 <= can_id <= 0x77F and dlc >= 1:
                node_id = can_id - 0x700
                if node_id == NODE_ID:
                    state = data[0]
                    self.status.emit(f"HB state=0x{state:02X}")

class MainWindow(QtWidgets.QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("M56S Control (PySide6 + pyqtgraph)")

        # UI
        self.mode_combo = QtWidgets.QComboBox()
        self.mode_combo.addItem("Velocity", MODE_VELOCITY)
        self.mode_combo.addItem("Torque", MODE_TORQUE)

        self.target_label = QtWidgets.QLabel("Target (rpm)")
        self.target_spin = QtWidgets.QSpinBox()
        self.target_spin.setRange(-100000, 100000)
        self.target_spin.setValue(1000)

        self.torque_slope_spin = QtWidgets.QSpinBox()
        self.torque_slope_spin.setRange(0, 1_000_000)
        self.torque_slope_spin.setValue(5000)

        self.velocity_limit_spin = QtWidgets.QSpinBox()
        self.velocity_limit_spin.setRange(0, 1_000_000)
        self.velocity_limit_spin.setValue(10000)

        self.direction_combo = QtWidgets.QComboBox()
        self.direction_combo.addItem("CW", 1)
        self.direction_combo.addItem("CCW", -1)

        self.btn_connect = QtWidgets.QPushButton("Connect")
        self.btn_disconnect = QtWidgets.QPushButton("Disconnect")
        self.btn_start = QtWidgets.QPushButton("Start")
        self.btn_stop = QtWidgets.QPushButton("Stop")

        self.status = QtWidgets.QLabel("Disconnected")

        # Plot placeholder
        self.plot = pg.PlotWidget(title="Telemetry (placeholder)")
        self.plot.plot([0, 1, 2], [0, 0, 0])

        # Layout
        form = QtWidgets.QFormLayout()
        form.addRow("Mode", self.mode_combo)
        form.addRow(self.target_label, self.target_spin)
        form.addRow("Torque slope", self.torque_slope_spin)
        form.addRow("Velocity limit", self.velocity_limit_spin)
        form.addRow("Direction", self.direction_combo)

        btns = QtWidgets.QHBoxLayout()
        btns.addWidget(self.btn_connect)
        btns.addWidget(self.btn_disconnect)
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)

        layout = QtWidgets.QVBoxLayout(self)
        layout.addLayout(form)
        layout.addLayout(btns)
        layout.addWidget(self.status)
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
        self.target_spin.valueChanged.connect(self.worker.set_target)
        self.torque_slope_spin.valueChanged.connect(self.worker.set_torque_slope)
        self.velocity_limit_spin.valueChanged.connect(self.worker.set_velocity_limit)
        self.direction_combo.currentIndexChanged.connect(self._direction_changed)
        self.worker.status.connect(self.status.setText)

        # Init
        self._mode_changed(0)
        self.worker.set_target(self.target_spin.value())
        self.worker.set_torque_slope(self.torque_slope_spin.value())
        self.worker.set_velocity_limit(self.velocity_limit_spin.value())
        self.worker.set_direction(self.direction_combo.currentData())

    def _mode_changed(self, _):
        mode = self.mode_combo.currentData()
        self.worker.set_mode(mode)
        if mode == MODE_VELOCITY:
            self.target_label.setText("Target (rpm)")
        else:
            self.target_label.setText("Target (raw torque)")
        self.worker.set_target(self.target_spin.value())

    def _direction_changed(self, _):
        self.worker.set_direction(self.direction_combo.currentData())
        self.worker.set_target(self.target_spin.value())

    def closeEvent(self, event):
        self.worker.disconnect_bus()
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