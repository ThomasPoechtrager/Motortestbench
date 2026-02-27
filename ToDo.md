# Motortestbench


# Motortestbench
- Änderungen nach Rückmeldung von Moons einarbeiten - Torque berechnung und Watchdog
- Limit Konstanten nicht im Raw Wert sondern im Metrischen Werten
- Saubere Sollwert übernahme
- Timout trotz 1000ms immer wieder sporadisch - warum? 
- Synchron Modus bauen


## Done
- Motor control with Start/Stop
- PDO integration (DC Bus Voltage, Temperatures)
- Safety features (Fault detection, Emergency disconnect)
- Connection robustness (Reconnection after CAN loss)
- NMT Heartbeat receiver (monitoring drive state)
- kg-based torque input with motor conversion
- Temperature and voltage telemetry display
- Removed torque from TPDO
- Activated watchdog in Drive controller

## Future
