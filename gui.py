import sys
import time
from collections import deque
from typing import Any, cast

from PySide6 import QtCore, QtWidgets
import pyqtgraph as pg

from config import (
    MODE_POSITION, MODE_TORQUE, MODE_VELOCITY
)
from gui_data import GuiState
from drive_data import DriveState
from Control import ControlLayer
from m56s_drive_ctrl import DriveController

class MainWindow(QtWidgets.QWidget):
    shutdown_worker = QtCore.Signal()
    shutdown_control = QtCore.Signal()

    ERROR_CODE_TEXTS = {
        0x0000: "Kein Fehler",
        0x08000000: "Kommunikationsfehler / Timeout",
        0x1000: "Generischer Fehler",
        0x2310: "Überstrom",
        0x3210: "Überspannung DC-Bus",
        0x3220: "Unterspannung DC-Bus",
        0x4210: "Übertemperatur",
        0x5113: "Geber/Sensor Fehler",
        0x5441: "Reglerfehler",
        0x6320: "Softwarefehler",
        0x7300: "Last-/Motorfehler",
        0x8110: "CAN Kommunikation",
        0x8120: "CAN in Error Passive",
        0x8130: "Heartbeat Fehler",
    }

    def __init__(self):
        super().__init__()
        self.setWindowTitle("M56S Control (PySide6 + pyqtgraph)")
        self.setMinimumSize(1200, 800)
        self._status_leds_motor1 = {}
        self._status_leds_motor2 = {}

        # UI
        self.mode_combo = QtWidgets.QComboBox()
        self.mode_combo.addItem("Velocity", MODE_VELOCITY)
        self.mode_combo.addItem("Torque", MODE_TORQUE)
        self.mode_combo.addItem("Position", MODE_POSITION)
        self.mode_combo.setCurrentIndex(1)

        self.mode_combo_m2 = QtWidgets.QComboBox()
        self.mode_combo_m2.addItem("Velocity", MODE_VELOCITY)
        self.mode_combo_m2.addItem("Torque", MODE_TORQUE)
        self.mode_combo_m2.addItem("Position", MODE_POSITION)
        self.mode_combo_m2.setCurrentIndex(1)

        self.target_velocity_label = QtWidgets.QLabel("Target velocity (rpm)")
        self.target_velocity_spin = QtWidgets.QSpinBox()
        self.target_velocity_spin.setRange(-100000, 100000)
        self.target_velocity_spin.setValue(1000)
        self.target_velocity_spin.setKeyboardTracking(False)

        self.target_velocity_spin_m2 = QtWidgets.QSpinBox()
        self.target_velocity_spin_m2.setRange(-100000, 100000)
        self.target_velocity_spin_m2.setValue(1000)
        self.target_velocity_spin_m2.setKeyboardTracking(False)

        self.target_torque_label = QtWidgets.QLabel("Target load (kg)")
        self.target_torque_spin = QtWidgets.QSpinBox()
        self.target_torque_spin.setRange(0, 100)
        self.target_torque_spin.setValue(0)
        self.target_torque_spin.setKeyboardTracking(False)

        self.target_torque_spin_m2 = QtWidgets.QSpinBox()
        self.target_torque_spin_m2.setRange(0, 100)
        self.target_torque_spin_m2.setValue(0)
        self.target_torque_spin_m2.setKeyboardTracking(False)

        self.target_position_label = QtWidgets.QLabel("Target position (cm)")
        self.target_position_spin = QtWidgets.QDoubleSpinBox()
        self.target_position_spin.setRange(-10000, 10000)
        self.target_position_spin.setDecimals(0)
        self.target_position_spin.setSingleStep(1)
        self.target_position_spin.setValue(0)
        self.target_position_spin.setKeyboardTracking(False)

        self.target_position_spin_m2 = QtWidgets.QDoubleSpinBox()
        self.target_position_spin_m2.setRange(-10000, 10000)
        self.target_position_spin_m2.setDecimals(0)
        self.target_position_spin_m2.setSingleStep(1)
        self.target_position_spin_m2.setValue(0)
        self.target_position_spin_m2.setKeyboardTracking(False)

        self.profile_velocity_label = QtWidgets.QLabel("Profile velocity (rpm)")
        self.profile_velocity_spin = QtWidgets.QSpinBox()
        self.profile_velocity_spin.setRange(0, 100000)
        self.profile_velocity_spin.setValue(500)
        self.profile_velocity_spin.setKeyboardTracking(False)

        self.profile_velocity_spin_m2 = QtWidgets.QSpinBox()
        self.profile_velocity_spin_m2.setRange(0, 100000)
        self.profile_velocity_spin_m2.setValue(500)
        self.profile_velocity_spin_m2.setKeyboardTracking(False)

        self.torque_slope_spin = QtWidgets.QSpinBox()
        self.torque_slope_spin.setRange(0, 1_000_000)
        self.torque_slope_spin.setValue(50)

        self.torque_slope_spin_m2 = QtWidgets.QSpinBox()
        self.torque_slope_spin_m2.setRange(0, 1_000_000)
        self.torque_slope_spin_m2.setValue(50)

        self.velocity_limit_spin = QtWidgets.QSpinBox()
        self.velocity_limit_spin.setRange(0, 10000)
        self.velocity_limit_spin.setValue(60)

        self.velocity_limit_spin_m2 = QtWidgets.QSpinBox()
        self.velocity_limit_spin_m2.setRange(0, 10000)
        self.velocity_limit_spin_m2.setValue(60)

        self.cycle_time_spin = QtWidgets.QSpinBox()
        self.cycle_time_spin.setRange(1, 1000)
        self.cycle_time_spin.setValue(20)
        self.cycle_time_spin.setKeyboardTracking(False)

        self.cycle_time_spin_m2 = QtWidgets.QSpinBox()
        self.cycle_time_spin_m2.setRange(1, 1000)
        self.cycle_time_spin_m2.setValue(20)
        self.cycle_time_spin_m2.setKeyboardTracking(False)

        self.direction_combo = QtWidgets.QComboBox()
        self.direction_combo.addItem("CW", 1)
        self.direction_combo.addItem("CCW", -1)
        self.direction_combo.setCurrentIndex(1)

        self.direction_combo_m2 = QtWidgets.QComboBox()
        self.direction_combo_m2.addItem("CW", 1)
        self.direction_combo_m2.addItem("CCW", -1)
        self.direction_combo_m2.setCurrentIndex(1)

        self.btn_connect = QtWidgets.QPushButton("Connect M1")
        self.btn_disconnect = QtWidgets.QPushButton("Disconnect M1")
        self.btn_start = QtWidgets.QPushButton("Start M1")
        self.btn_stop = QtWidgets.QPushButton("Stop M1")
        self.btn_fault_reset = QtWidgets.QPushButton("Fault reset M1")

        self.btn_connect_m2 = QtWidgets.QPushButton("Connect M2")
        self.btn_disconnect_m2 = QtWidgets.QPushButton("Disconnect M2")
        self.btn_start_m2 = QtWidgets.QPushButton("Start M2")
        self.btn_stop_m2 = QtWidgets.QPushButton("Stop M2")
        self.btn_fault_reset_m2 = QtWidgets.QPushButton("Fault reset M2")

        self.status = QtWidgets.QLabel("Disconnected")
        self.tpdo1_label = QtWidgets.QLabel("TPDO1: status=0x---- err=0x---- mode=--")
        self.tpdo2_label = QtWidgets.QLabel("TPDO2: pos=0 vel=0 rpm")
        self.tpdo3_label = QtWidgets.QLabel("TPDO3: current=0 torque=0")
        self.tpdo4_label = QtWidgets.QLabel(
            "DC Bus: 0.0V (max: 0.0V) | Temps: Drive=0Â°C Chassis=0Â°C"
        )
        self.error_code_label = QtWidgets.QLabel("Fehlercode")
        self.error_code_output = QtWidgets.QLineEdit()
        self.error_code_output.setReadOnly(True)
        self.error_code_output.setText("0x0000 - Kein Fehler")
        self.statusword_label = QtWidgets.QLabel("Statusword: 0x----")

        self.motor2_header = QtWidgets.QLabel("Motor 2 (Node 2)")
        self.motor2_tpdo1_label = QtWidgets.QLabel("TPDO1: status=0x---- err=0x---- mode=--")
        self.motor2_tpdo2_label = QtWidgets.QLabel("TPDO2: pos=0 vel=0 rpm")
        self.motor2_tpdo3_label = QtWidgets.QLabel("TPDO3: current=0 torque=0")
        self.motor2_tpdo4_label = QtWidgets.QLabel(
            "DC Bus: 0.0V (max: 0.0V) | Temps: Drive=0Â°C Chassis=0Â°C"
        )
        self.motor2_error_code_output = QtWidgets.QLineEdit()
        self.motor2_error_code_output.setReadOnly(True)
        self.motor2_error_code_output.setText("0x0000 - Kein Fehler")
        self.motor2_statusword_label = QtWidgets.QLabel("Statusword: 0x----")
        self.status_bits_group = self._build_status_bits("Statusword bits M1", self._status_leds_motor1)
        self.status_bits_group_m2 = self._build_status_bits("Statusword bits M2", self._status_leds_motor2)

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
        plot_item = self.plot.getPlotItem()
        if plot_item is None:
            raise RuntimeError("Plot item not available")
        self._plot_item = plot_item
        vb = self._plot_item.vb
        if vb is None:
            raise RuntimeError("Plot view box not available")
        self._plot_vb = cast(Any, vb)
        self._plot_item.setLabel("left", "Position", units="cm")
        self._plot_item.showAxis("right")
        self._plot_item.getAxis("right").setLabel("Speed", units="cm/s")

        self._vel_view = pg.ViewBox()
        self._plot_item.scene().addItem(self._vel_view)
        self._plot_item.getAxis("right").linkToView(self._vel_view)
        self._vel_view.setXLink(self._plot_item)

        self._torque_axis = pg.AxisItem(orientation="right")
        self._torque_axis.setLabel("Torque", units="mNm")
        plot_layout = cast(Any, self._plot_item).layout
        plot_layout.addItem(self._torque_axis, 1, 3)

        self._torque_view = pg.ViewBox()
        self._plot_item.scene().addItem(self._torque_view)
        self._torque_axis.linkToView(self._torque_view)
        self._torque_view.setXLink(self._plot_item)

        self._plot_vb.sigResized.connect(self._update_viewboxes)
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
        form.addRow("Target velocity (rpm)", self.target_velocity_spin)
        form.addRow("Target load (kg)", self.target_torque_spin)
        form.addRow("Target position (cm)", self.target_position_spin)
        form.addRow("Profile velocity (rpm)", self.profile_velocity_spin)
        form.addRow("Torque slope (0.1%/s)", self.torque_slope_spin)
        form.addRow("Velocity limit (motor rpm)", self.velocity_limit_spin)
        form.addRow("Direction", self.direction_combo)
        form.addRow("Cycle time (ms)", self.cycle_time_spin)

        form_m2 = QtWidgets.QFormLayout()
        form_m2.addRow("Mode", self.mode_combo_m2)
        form_m2.addRow("Target velocity (rpm)", self.target_velocity_spin_m2)
        form_m2.addRow("Target load (kg)", self.target_torque_spin_m2)
        form_m2.addRow("Target position (cm)", self.target_position_spin_m2)
        form_m2.addRow("Profile velocity (rpm)", self.profile_velocity_spin_m2)
        form_m2.addRow("Torque slope (0.1%/s)", self.torque_slope_spin_m2)
        form_m2.addRow("Velocity limit (motor rpm)", self.velocity_limit_spin_m2)
        form_m2.addRow("Direction", self.direction_combo_m2)
        form_m2.addRow("Cycle time (ms)", self.cycle_time_spin_m2)

        btns = QtWidgets.QHBoxLayout()
        btns.addWidget(self.btn_connect)
        btns.addWidget(self.btn_disconnect)
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)
        btns.addWidget(self.btn_fault_reset)

        btns_m2 = QtWidgets.QHBoxLayout()
        btns_m2.addWidget(self.btn_connect_m2)
        btns_m2.addWidget(self.btn_disconnect_m2)
        btns_m2.addWidget(self.btn_start_m2)
        btns_m2.addWidget(self.btn_stop_m2)
        btns_m2.addWidget(self.btn_fault_reset_m2)

        layout = QtWidgets.QVBoxLayout(self)
        motor_controls_row = QtWidgets.QHBoxLayout()
        motor1_col = QtWidgets.QVBoxLayout()
        motor1_col.addWidget(QtWidgets.QLabel("Motor 1 Sollwerte"))
        motor1_col.addLayout(form)
        motor1_col.addLayout(btns)

        motor2_col = QtWidgets.QVBoxLayout()
        motor2_col.addWidget(QtWidgets.QLabel("Motor 2 Sollwerte"))
        motor2_col.addLayout(form_m2)
        motor2_col.addLayout(btns_m2)

        motor_controls_row.addLayout(motor1_col)
        motor_controls_row.addLayout(motor2_col)
        layout.addLayout(motor_controls_row)
        layout.addWidget(self.status)
        status_row = QtWidgets.QHBoxLayout()
        status_row.addWidget(self.status_bits_group)

        status_details = QtWidgets.QVBoxLayout()
        status_details.addWidget(QtWidgets.QLabel("Motor 1 (Node 1)"))
        status_details.addWidget(self.tpdo1_label)
        status_details.addWidget(self.tpdo2_label)
        status_details.addWidget(self.tpdo3_label)
        status_details.addWidget(self.tpdo4_label)
        status_details.addWidget(self.error_code_label)
        status_details.addWidget(self.error_code_output)
        status_details.addWidget(self.statusword_label)
        status_details.addSpacing(8)
        status_details.addWidget(self.motor2_header)
        status_details.addWidget(self.motor2_tpdo1_label)
        status_details.addWidget(self.motor2_tpdo2_label)
        status_details.addWidget(self.motor2_tpdo3_label)
        status_details.addWidget(self.motor2_tpdo4_label)
        status_details.addWidget(QtWidgets.QLabel("Fehlercode"))
        status_details.addWidget(self.motor2_error_code_output)
        status_details.addWidget(self.motor2_statusword_label)
        status_details.addStretch(1)

        status_row.addLayout(status_details)
        status_row.addWidget(self.status_bits_group_m2)
        layout.addLayout(status_row)
        plot_controls = QtWidgets.QHBoxLayout()
        plot_controls.addWidget(self.btn_reset_plot)
        plot_controls.addWidget(self.btn_pause_plot)
        plot_controls.addStretch(1)
        layout.addLayout(plot_controls)
        layout.addWidget(self.plot)

        # Architecture: GUI -> gui_state -> Control -> {drive_state_motor1, drive_state_motor2} -> shared MotorDriver
        self.gui_state_motor1 = GuiState()
        self.gui_state_motor2 = GuiState()
        self.drive_state_motor1 = DriveState()
        self.drive_state_motor2 = DriveState()
        self.drive_state = self.drive_state_motor1  # Backward compat for now

        # Control layer thread
        self.control_thread = QtCore.QThread(self)
        self.control_layer = ControlLayer(
            {1: self.gui_state_motor1, 2: self.gui_state_motor2},
            {1: self.drive_state_motor1, 2: self.drive_state_motor2}
        )
        self.control_layer.moveToThread(self.control_thread)
        self.control_thread.started.connect(self.control_layer.start_control)
        self.shutdown_control.connect(
            self.control_layer.shutdown,
            QtCore.Qt.ConnectionType.QueuedConnection
        )

        # Drive controller thread - shared bus for motor 1 + 2
        self.worker_thread = QtCore.QThread(self)
        self.controller = DriveController({1: self.drive_state_motor1, 2: self.drive_state_motor2})
        self.controller.moveToThread(self.worker_thread)
        self.worker_thread.started.connect(self.controller.start_polling)
        self.shutdown_worker.connect(
            self.controller.shutdown,
            QtCore.Qt.ConnectionType.QueuedConnection
        )

        # Signals
        self.btn_connect.clicked.connect(self._on_connect_clicked)
        self.btn_disconnect.clicked.connect(self._on_disconnect_clicked)
        self.btn_start.clicked.connect(self._on_start_clicked)
        self.btn_stop.clicked.connect(self._on_stop_clicked)
        self.btn_fault_reset.clicked.connect(self._on_fault_reset_clicked)
        self.btn_connect_m2.clicked.connect(self._on_connect_clicked_m2)
        self.btn_disconnect_m2.clicked.connect(self._on_disconnect_clicked_m2)
        self.btn_start_m2.clicked.connect(self._on_start_clicked_m2)
        self.btn_stop_m2.clicked.connect(self._on_stop_clicked_m2)
        self.btn_fault_reset_m2.clicked.connect(self._on_fault_reset_clicked_m2)
        self.mode_combo.currentIndexChanged.connect(self._mode_changed)
        self.mode_combo_m2.currentIndexChanged.connect(self._mode_changed_m2)
        self.target_velocity_spin.editingFinished.connect(self._target_velocity_committed)
        self.target_velocity_spin_m2.editingFinished.connect(self._target_velocity_committed_m2)
        self.target_torque_spin.editingFinished.connect(self._target_torque_committed)
        self.target_torque_spin_m2.editingFinished.connect(self._target_torque_committed_m2)
        self.target_position_spin.editingFinished.connect(self._target_position_committed)
        self.target_position_spin_m2.editingFinished.connect(self._target_position_committed_m2)
        self.profile_velocity_spin.editingFinished.connect(self._profile_velocity_committed)
        self.profile_velocity_spin_m2.editingFinished.connect(self._profile_velocity_committed_m2)
        self.torque_slope_spin.editingFinished.connect(self._torque_slope_committed)
        self.torque_slope_spin_m2.editingFinished.connect(self._torque_slope_committed_m2)
        self.velocity_limit_spin.editingFinished.connect(self._velocity_limit_committed)
        self.velocity_limit_spin_m2.editingFinished.connect(self._velocity_limit_committed_m2)
        self.cycle_time_spin.editingFinished.connect(self._cycle_committed)
        self.cycle_time_spin_m2.editingFinished.connect(self._cycle_committed_m2)
        self.direction_combo.currentIndexChanged.connect(self._direction_changed)
        self.direction_combo_m2.currentIndexChanged.connect(self._direction_changed_m2)
        self.btn_reset_plot.clicked.connect(self._reset_plot)
        self.btn_pause_plot.clicked.connect(self._toggle_plot_pause)
        self.controller.status.connect(self.status.setText)

        self._ui_timer = QtCore.QTimer(self)
        self._ui_timer.setInterval(100)
        self._ui_timer.timeout.connect(self._refresh_from_state)
        self._ui_timer.start()

        # Init
        self._mode_changed(self.mode_combo.currentIndex())
        self._target_velocity_committed()
        self._target_torque_committed()
        self._target_position_committed()
        self._profile_velocity_committed()
        self._torque_slope_committed()
        self._velocity_limit_committed()
        self._cycle_committed()
        self.gui_state_motor1.update_command(direction=self.direction_combo.currentData())

        self._mode_changed_m2(self.mode_combo_m2.currentIndex())
        self._target_velocity_committed_m2()
        self._target_torque_committed_m2()
        self._target_position_committed_m2()
        self._profile_velocity_committed_m2()
        self._torque_slope_committed_m2()
        self._velocity_limit_committed_m2()
        self._cycle_committed_m2()
        self.gui_state_motor2.update_command(direction=self.direction_combo_m2.currentData())

        # Start control layer and drive controller immediately
        self.control_thread.start()
        self.worker_thread.start()

    def _mode_changed(self, _):
        mode = self.mode_combo.currentData()
        self.gui_state_motor1.update_command(mode=mode)
        self.target_velocity_spin.setEnabled(mode in (MODE_VELOCITY, MODE_POSITION))
        self.target_torque_spin.setEnabled(True)
        self.target_position_spin.setEnabled(mode == MODE_POSITION)
        self.profile_velocity_spin.setEnabled(mode == MODE_POSITION)
        self._target_velocity_committed()
        self._target_torque_committed()
        self._target_position_committed()
        self._profile_velocity_committed()

    def _direction_changed(self, _):
        self.gui_state_motor1.update_command(direction=self.direction_combo.currentData())
        self._target_velocity_committed()
        self._target_torque_committed()

    def _target_velocity_committed(self):
        self.target_velocity_spin.interpretText()
        self.gui_state_motor1.update_command(target_velocity_rpm=self.target_velocity_spin.value())

    def _target_torque_committed(self):
        self.target_torque_spin.interpretText()
        # Store as mkg (kg Ã— 1000) for precision in integer field
        kg_value = self.target_torque_spin.value()
        self.gui_state_motor1.update_command(target_torque_mnm=kg_value * 1000)


    def _target_position_committed(self):
        # Increment counter to trigger new setpoint even if value didn't change
        snapshot = self.gui_state_motor1.snapshot()
        new_counter = snapshot.command.position_setpoint_counter + 1
        self.gui_state_motor1.update_command(
            target_position_cm=self.target_position_spin.value(),
            position_setpoint_counter=new_counter
        )

    def _profile_velocity_committed(self):
        self.profile_velocity_spin.interpretText()
        self.gui_state_motor1.update_command(profile_velocity_rpm=self.profile_velocity_spin.value())

    def _cycle_committed(self):
        self.cycle_time_spin.interpretText()
        self.gui_state_motor1.update_command(cycle_time_ms=self.cycle_time_spin.value())

    def _torque_slope_committed(self):
        self.torque_slope_spin.interpretText()
        self.gui_state_motor1.update_command(torque_slope=self.torque_slope_spin.value())

    def _velocity_limit_committed(self):
        self.velocity_limit_spin.interpretText()
        self.gui_state_motor1.update_command(velocity_limit_rpm=self.velocity_limit_spin.value())

    def _mode_changed_m2(self, _):
        mode = self.mode_combo_m2.currentData()
        self.gui_state_motor2.update_command(mode=mode)
        self.target_velocity_spin_m2.setEnabled(mode in (MODE_VELOCITY, MODE_POSITION))
        self.target_torque_spin_m2.setEnabled(True)
        self.target_position_spin_m2.setEnabled(mode == MODE_POSITION)
        self.profile_velocity_spin_m2.setEnabled(mode == MODE_POSITION)
        self._target_velocity_committed_m2()
        self._target_torque_committed_m2()
        self._target_position_committed_m2()
        self._profile_velocity_committed_m2()

    def _direction_changed_m2(self, _):
        self.gui_state_motor2.update_command(direction=self.direction_combo_m2.currentData())
        self._target_velocity_committed_m2()
        self._target_torque_committed_m2()

    def _target_velocity_committed_m2(self):
        self.target_velocity_spin_m2.interpretText()
        self.gui_state_motor2.update_command(target_velocity_rpm=self.target_velocity_spin_m2.value())

    def _target_torque_committed_m2(self):
        self.target_torque_spin_m2.interpretText()
        kg_value = self.target_torque_spin_m2.value()
        self.gui_state_motor2.update_command(target_torque_mnm=kg_value * 1000)

    def _target_position_committed_m2(self):
        snapshot = self.gui_state_motor2.snapshot()
        new_counter = snapshot.command.position_setpoint_counter + 1
        self.gui_state_motor2.update_command(
            target_position_cm=self.target_position_spin_m2.value(),
            position_setpoint_counter=new_counter
        )

    def _profile_velocity_committed_m2(self):
        self.profile_velocity_spin_m2.interpretText()
        self.gui_state_motor2.update_command(profile_velocity_rpm=self.profile_velocity_spin_m2.value())

    def _cycle_committed_m2(self):
        self.cycle_time_spin_m2.interpretText()
        self.gui_state_motor2.update_command(cycle_time_ms=self.cycle_time_spin_m2.value())

    def _torque_slope_committed_m2(self):
        self.torque_slope_spin_m2.interpretText()
        self.gui_state_motor2.update_command(torque_slope=self.torque_slope_spin_m2.value())

    def _velocity_limit_committed_m2(self):
        self.velocity_limit_spin_m2.interpretText()
        self.gui_state_motor2.update_command(velocity_limit_rpm=self.velocity_limit_spin_m2.value())

    def _refresh_from_state(self):
        snapshot = self.gui_state_motor1.snapshot()
        fb = snapshot.feedback
        self.tpdo1_label.setText(
            f"TPDO1: status=0x{fb.statusword:04X} err=0x{fb.error_code:04X} mode={fb.mode_display}"
        )
        self.statusword_label.setText(f"Statusword: 0x{fb.statusword:04X}")
        self._update_status_bits(fb.statusword, self._status_leds_motor1)

        self.tpdo2_label.setText(
            (
                f"TPDO2: pos={fb.position_cm:.2f} cm vel={fb.speed_cm_s:.2f} cm/s "
                f"({fb.speed_rpm:.0f} rpm)"
            )
        )

        torque_nm = fb.torque_mnm / 1000.0
        self.tpdo3_label.setText(
            f"TPDO3: current={fb.current_a:.2f} A torque={torque_nm:.2f} Nm"
        )

        self.tpdo4_label.setText(
            f"DC Bus: {fb.dc_bus_voltage:.1f}V (max: {fb.dc_bus_voltage_max:.1f}V) | "
            f"Temps: Drive={fb.drive_temperature:.1f}Â°C Chassis={fb.chassis_temperature:.1f}Â°C"
        )
        self.error_code_output.setText(self._format_error_code(fb.error_code))

        self._last_pos_cm = fb.position_cm
        self._last_vel_cm_s = fb.speed_cm_s
        self._last_torque_mnm = fb.torque_mnm

        motor2_snapshot = self.drive_state_motor2.snapshot()
        motor2_fb = motor2_snapshot.feedback
        self.motor2_tpdo1_label.setText(
            f"TPDO1: status=0x{motor2_fb.statusword:04X} err=0x{motor2_fb.error_code:04X} mode={motor2_fb.mode_display}"
        )
        self.motor2_tpdo2_label.setText(
            (
                f"TPDO2: pos={motor2_fb.position_cm:.2f} cm vel={motor2_fb.speed_cm_s:.2f} cm/s "
                f"({motor2_fb.speed_rpm:.0f} rpm)"
            )
        )
        motor2_torque_nm = motor2_fb.torque_mnm / 1000.0
        self.motor2_tpdo3_label.setText(
            f"TPDO3: current={motor2_fb.current_a:.2f} A torque={motor2_torque_nm:.2f} Nm"
        )
        self.motor2_tpdo4_label.setText(
            f"DC Bus: {motor2_fb.dc_bus_voltage:.1f}V (max: {motor2_fb.dc_bus_voltage_max:.1f}V) | "
            f"Temps: Drive={motor2_fb.drive_temperature:.1f}Â°C Chassis={motor2_fb.chassis_temperature:.1f}Â°C"
        )
        self.motor2_error_code_output.setText(self._format_error_code(motor2_fb.error_code))
        self.motor2_statusword_label.setText(f"Statusword: 0x{motor2_fb.statusword:04X}")
        self._update_status_bits(motor2_fb.statusword, self._status_leds_motor2)

        self._append_plot()

    def _format_error_code(self, error_code: int) -> str:
        text = self.ERROR_CODE_TEXTS.get(error_code, "Unbekannter Fehlercode")
        return f"0x{error_code:04X} - {text}"

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
        cast(Any, self._plot_item).setXRange(0.0, x_max, padding=0.0)

    def _update_viewboxes(self):
        self._vel_view.setGeometry(self._plot_vb.sceneBoundingRect())
        self._vel_view.linkedViewChanged(self._plot_vb, self._vel_view.XAxis)
        self._torque_view.setGeometry(self._plot_vb.sceneBoundingRect())
        self._torque_view.linkedViewChanged(self._plot_vb, self._torque_view.XAxis)

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
        cast(Any, self._plot_item).setXRange(0.0, self._plot_window_s, padding=0.0)

    def _toggle_plot_pause(self):
        self._plot_paused = not self._plot_paused
        self.btn_pause_plot.setText("Resume plot" if self._plot_paused else "Pause plot")

    def _on_connect_clicked(self):
        self.gui_state_motor1.update_flags(request_connect=True)

    def _on_disconnect_clicked(self):
        self.gui_state_motor1.update_flags(request_disconnect=True)
        # Don't stop the thread, just disconnect
        # The polling thread keeps running for faster reconnection

    def _on_start_clicked(self):
        self.gui_state_motor1.update_flags(request_start=True)

    def _on_stop_clicked(self):
        self.gui_state_motor1.update_flags(request_stop=True)

    def _on_fault_reset_clicked(self):
        self.gui_state_motor1.update_flags(request_fault_reset=True)

    def _on_connect_clicked_m2(self):
        self.gui_state_motor2.update_flags(request_connect=True)

    def _on_disconnect_clicked_m2(self):
        self.gui_state_motor2.update_flags(request_disconnect=True)

    def _on_start_clicked_m2(self):
        self.gui_state_motor2.update_flags(request_start=True)

    def _on_stop_clicked_m2(self):
        self.gui_state_motor2.update_flags(request_stop=True)

    def _on_fault_reset_clicked_m2(self):
        self.gui_state_motor2.update_flags(request_fault_reset=True)

    def _build_status_bits(self, title: str, led_store: dict):
        group = QtWidgets.QGroupBox(title)
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
            led_store[bit] = led

        grid.setColumnStretch(2, 0)
        return group

    def _update_status_bits(self, statusword: int, led_store: dict):
        for bit, led in led_store.items():
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
        if self.worker_thread.isRunning():
            self.gui_state_motor1.update_flags(request_disconnect=True)
            self.gui_state_motor2.update_flags(request_disconnect=True)
            QtCore.QThread.msleep(100)
            self.shutdown_worker.emit()
            QtCore.QThread.msleep(100)
            self.worker_thread.quit()
            self.worker_thread.wait(1000)
        if self.control_thread.isRunning():
            self.shutdown_control.emit()
            QtCore.QThread.msleep(50)
            self.control_thread.quit()
            self.control_thread.wait(1000)
        super().closeEvent(event)

def main():
    app = QtWidgets.QApplication(sys.argv)
    w = MainWindow()
    w.resize(1400, 900)
    w.show()
    try:
        return app.exec()
    except KeyboardInterrupt:
        if w.isVisible():
            w.close()
        return 0

if __name__ == "__main__":
    sys.exit(main())
