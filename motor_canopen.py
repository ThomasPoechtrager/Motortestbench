import canopen
import time

class MotorCANopen:
    def __init__(self, channel, node_id, bitrate=500000):
        self.network = canopen.Network()
        # Use 'pcan' for Windows USB-to-CAN adapters (e.g., Peak PCAN)
        self.network.connect(channel=channel, bustype='pcan', bitrate=bitrate)
        self.node = canopen.RemoteNode(node_id, 'CiA402.eds')  # You must provide the correct EDS file
        self.network.add_node(self.node)
        self.mode = None

    def set_mode(self, mode):
        """Set operation mode: 'rpm' or 'torque'"""
        if mode == 'rpm':
            self.node.sdo[0x6060].raw = 3  # Profile velocity mode
            self.mode = 'rpm'
        elif mode == 'torque':
            self.node.sdo[0x6060].raw = 4  # Torque mode
            self.mode = 'torque'
        else:
            raise ValueError('Mode must be "rpm" or "torque"')
        time.sleep(0.1)

    def enable_motor(self):
        # Standard CiA 402 state machine: shutdown -> switch on -> enable operation
        self.node.sdo[0x6040].raw = 0x06
        time.sleep(0.1)
        self.node.sdo[0x6040].raw = 0x07
        time.sleep(0.1)
        self.node.sdo[0x6040].raw = 0x0F
        time.sleep(0.1)

    def set_rpm(self, rpm):
        if self.mode != 'rpm':
            raise RuntimeError('Motor not in rpm mode!')
        self.node.sdo[0x60FF].raw = int(rpm)

    def set_torque(self, torque):
        if self.mode != 'torque':
            raise RuntimeError('Motor not in torque mode!')
        self.node.sdo[0x6071].raw = int(torque)

    def read_pdo(self):
        # Assumes PDO mapping: 0x2028: rpm, 0x2027: current, 0x6077: torque
        rpm = self.node.rpdo[1]['Velocity actual value'].raw if 'Velocity actual value' in self.node.rpdo[1] else None
        current = self.node.rpdo[1]['Current actual value'].raw if 'Current actual value' in self.node.rpdo[1] else None
        torque = self.node.rpdo[1]['Torque actual value'].raw if 'Torque actual value' in self.node.rpdo[1] else None
        return rpm, current, torque

    def close(self):
        self.network.disconnect()


def testbench():
    channel = input('Enter CAN interface (e.g., can0): ')
    node_id = int(input('Enter motor node ID: '))
    motor = MotorCANopen(channel, node_id)
    motor.enable_motor()
    while True:
        mode = input('Select mode (rpm/torque/exit): ').strip()
        if mode == 'exit':
            break
        motor.set_mode(mode)
        if mode == 'rpm':
            rpm = int(input('Enter desired RPM: '))
            motor.set_rpm(rpm)
        elif mode == 'torque':
            torque = int(input('Enter desired torque: '))
            motor.set_torque(torque)
        for _ in range(5):
            rpm_val, current_val, torque_val = motor.read_pdo()
            print(f'RPM: {rpm_val}, Current: {current_val}, Torque: {torque_val}')
            time.sleep(0.5)
    motor.close()

if __name__ == '__main__':
    testbench()
