import serial
import time

COM_PORT = "COM17"
SERIAL_BAUD = 2_000_000  # Waveshare default serial baud
CAN_BITRATE_CODE = 0x01  # 1 Mbps
NODE_ID = 1

# CANopen COB-IDs
COB_NMT          = 0x000
COB_SDO_RX_BASE  = 0x600
COB_SDO_TX_BASE  = 0x580
COB_RPDO1_BASE   = 0x200
COB_RPDO2_BASE   = 0x300
COB_RPDO3_BASE   = 0x400
COB_TPDO1_BASE   = 0x180
COB_HEARTBEAT    = 0x700

# CiA402 Controlword
CW_SHUTDOWN         = 0x0006
CW_SWITCH_ON        = 0x0007
CW_ENABLE_OPERATION = 0x000F
CW_HALT_BIT         = 0x0100  # bit8

# Modes of Operation
MODE_TORQUE   = 4
MODE_VELOCITY = 3

TARGET_VELOCITY = 1000  # TODO: Einheit bestätigen (rpm? raw?)

# === Fixed-20 helpers ===

def checksum_low8(data: bytes) -> int:
    return sum(data) & 0xFF

def build_can_frame_fixed20(can_id: int, data: bytes) -> bytes:
    """
    Build a Waveshare fixed-20 CAN data frame.
    """
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
    """
    Waveshare fixed-20 configuration frame.
    """
    frame = bytearray(20)
    frame[0] = 0xAA
    frame[1] = 0x55
    frame[2] = 0x12  # config frame type
    frame[3] = can_bitrate_code & 0xFF
    frame[4] = frame_type & 0xFF
    frame[5] = can_mode & 0xFF
    frame[6] = auto_retx & 0xFF
    frame[7:11]  = filter_id.to_bytes(4, "little", signed=False)
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

def read_frame_fixed20(ser: serial.Serial, timeout_s: float = 0.2):
    t_end = time.time() + timeout_s
    while time.time() < t_end:
        b = ser.read(1)
        if not b:
            continue
        if b[0] == 0xAA:
            b2 = ser.read(1)
            if b2 and b2[0] == 0x55:
                rest = ser.read(18)
                if len(rest) != 18:
                    return None
                frame = bytes([0xAA, 0x55]) + rest
                cs_calc = checksum_low8(frame[2:19])
                if cs_calc != frame[19]:
                    return None
                return frame
    return None

def parse_fixed20(frame: bytes):
    f_type = frame[3]
    f_format = frame[4]
    can_id = int.from_bytes(frame[5:9], byteorder="little", signed=False)
    dlc = frame[9] & 0x0F
    data = frame[10:10+dlc]
    return can_id, f_type, f_format, dlc, data

# === SDO helpers ===

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

# === NMT ===

def nmt_send(ser, command, node_id):
    send_can(ser, COB_NMT, bytes([command, node_id]))

# === PDO ===

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

def rpdo3_send(ser, target_velocity):
    data = int(target_velocity).to_bytes(4, "little", signed=True)
    send_can(ser, COB_RPDO3_BASE + NODE_ID, data)

# === Drive control ===

def enable_drive_sdo(ser):
    sdo_write_u16(ser, 0x6040, 0x00, 0x0006); time.sleep(0.02)
    sdo_write_u16(ser, 0x6040, 0x00, 0x0007); time.sleep(0.02)
    sdo_write_u16(ser, 0x6040, 0x00, 0x000F)

def disable_drive_sdo(ser):
    sdo_write_u16(ser, 0x6040, 0x00, 0x0006)

# === Main ===

def main():
    print(f"Opening {COM_PORT} @ {SERIAL_BAUD} ...")
    with serial.Serial(COM_PORT, SERIAL_BAUD, timeout=0.05) as ser:
        cfg = build_config_cmd_fixed20(
            can_bitrate_code=CAN_BITRATE_CODE,
            frame_type=0x01,
            filter_id=0x00000000,
            mask_id=0x00000000,
            can_mode=0x00,
            auto_retx=0x00
        )
        ser.reset_input_buffer()
        ser.write(cfg)
        ser.flush()
        print("Sent FIXED20 config:", cfg.hex(" "))

        # Heartbeat producer time: 1000 ms
        sdo_write_u16(ser, 0x1017, 0x00, 1000)
        time.sleep(0.2)

        # Pre-Op -> Operational
        nmt_send(ser, 0x80, NODE_ID)
        time.sleep(0.2)

        nmt_send(ser, 0x01, NODE_ID)
        time.sleep(0.2)

        # Drive enable sequence (SDO)
        enable_drive_sdo(ser)
        time.sleep(0.1)

        # Default RPDO2 limits (safe)
        rpdo2_send(ser, torque_slope=5000, velocity_limit=10000)

        print("Ready. Commands: [s]=start, [x]=stop, [q]=quit")
        running = False

        while True:
            # Non-blocking CAN read (for heartbeat/TPDO debug)
            fr = read_frame_fixed20(ser, timeout_s=0.01)
            if fr:
                can_id, f_type, f_format, dlc, data = parse_fixed20(fr)
                if 0x700 <= can_id <= 0x77F and dlc >= 1:
                    node_id = can_id - 0x700
                    state = data[0]
                    if node_id == NODE_ID:
                        print(f"HB state=0x{state:02X}")

            cmd = input("> ").strip().lower()
            if cmd == "q":
                break
            elif cmd == "s":
                # Start velocity mode
                cw = CW_ENABLE_OPERATION
                rpdo1_send(ser, cw, MODE_VELOCITY, target_torque=0)
                time.sleep(0.02)
                rpdo3_send(ser, target_velocity=TARGET_VELOCITY)
                running = True
                print(f"Start (velocity={TARGET_VELOCITY})")
            elif cmd == "x":
                # Stop: halt + velocity=0, then optional shutdown
                cw = CW_ENABLE_OPERATION | CW_HALT_BIT
                rpdo1_send(ser, cw, MODE_VELOCITY, target_torque=0)
                time.sleep(0.02)
                rpdo3_send(ser, target_velocity=0)
                time.sleep(0.05)
                disable_drive_sdo(ser)
                running = False
                print("Stop")
            else:
                print("Commands: s/x/q")

        if running:
            # Safety stop on exit
            cw = CW_ENABLE_OPERATION | CW_HALT_BIT
            rpdo1_send(ser, cw, MODE_VELOCITY, target_torque=0)
            rpdo3_send(ser, target_velocity=0)
            disable_drive_sdo(ser)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")