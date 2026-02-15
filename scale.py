from config import (
	RPM_TO_RAW, INC_PER_REV, GEAR_RATIO, CM_PER_SHAFT_REV,
	RATED_TORQUE_NM, RATED_CURRENT_A,
	CURRENT_ACTUAL_PCT_PER_LSB, TORQUE_PCT_PER_LSB
)

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
