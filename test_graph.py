import sys
from PyQt5 import QtWidgets, QtCore
import pyqtgraph as pg
import time

class ValuePlotter(QtWidgets.QWidget):
	def __init__(self):
		super().__init__()
		self.setWindowTitle('Value Visualizer')
		self.layout = QtWidgets.QVBoxLayout(self)

		self.plot_widget = pg.PlotWidget()
		self.layout.addWidget(self.plot_widget)

		self.input_layout = QtWidgets.QHBoxLayout()
		self.value_input = QtWidgets.QLineEdit()
		self.value_input.setPlaceholderText('Enter value')
		self.input_layout.addWidget(self.value_input)
		self.add_button = QtWidgets.QPushButton('Add Value')
		self.input_layout.addWidget(self.add_button)
		self.layout.addLayout(self.input_layout)

		self.add_button.clicked.connect(self.add_value)

		self.x_data = []  # time
		self.y_data = []  # value
		self.start_time = time.time()
		self.plot = self.plot_widget.plot(self.x_data, self.y_data, pen='b', symbol='o')

	def add_value(self):
		try:
			value = float(self.value_input.text())
		except ValueError:
			QtWidgets.QMessageBox.warning(self, 'Invalid Input', 'Please enter a numeric value.')
			return
		current_time = time.time() - self.start_time
		self.x_data.append(current_time)
		self.y_data.append(value)
		self.plot.setData(self.x_data, self.y_data)
		self.value_input.clear()

def main():
	app = QtWidgets.QApplication(sys.argv)
	window = ValuePlotter()
	window.show()
	sys.exit(app.exec_())

if __name__ == '__main__':
	main()
