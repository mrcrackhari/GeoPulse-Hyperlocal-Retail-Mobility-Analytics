import numpy as np

# Store coordinates
STORE_1_LAT, STORE_1_LON = 28.6139, 77.2090
STORE_2_LAT, STORE_2_LON = 28.6200, 77.2150

def generate_planted_devices(num_days=30):
    """
    Generates GPS pings for specific cohorts to satisfy the 30% cannibalization rule.
    """
    pings = []
    
    # 1. Devices that ONLY visit Store 1 (3,400 devices)
    for i in range(3400):
        device_id = f"DEV-S1-{i}"
        for day in range(num_days):
            if np.random.rand() < 0.3:
                pings.append({"device_id": device_id, "lat": STORE_1_LAT + np.random.normal(0, 0.001), "lon": STORE_1_LON + np.random.normal(0, 0.001), "day": day})

    # 2. Devices that ONLY visit Store 2 (1,400 devices)
    for i in range(1400):
        device_id = f"DEV-S2-{i}"
        for day in range(num_days):
            if np.random.rand() < 0.3:
                pings.append({"device_id": device_id, "lat": STORE_2_LAT + np.random.normal(0, 0.001), "lon": STORE_2_LON + np.random.normal(0, 0.001), "day": day})

    # 3. Devices that visit BOTH (The 600 cannibalization devices)
    for i in range(600):
        device_id = f"DEV-BOTH-{i}"
        for day in range(num_days):
            if np.random.rand() < 0.3: 
                if np.random.rand() < 0.5:
                    pings.append({"device_id": device_id, "lat": STORE_1_LAT + np.random.normal(0, 0.001), "lon": STORE_1_LON + np.random.normal(0, 0.001), "day": day})
                else:
                    pings.append({"device_id": device_id, "lat": STORE_2_LAT + np.random.normal(0, 0.001), "lon": STORE_2_LON + np.random.normal(0, 0.001), "day": day})

    return pings