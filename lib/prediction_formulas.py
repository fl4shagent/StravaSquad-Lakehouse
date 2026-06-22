"""
Race-time prediction formulas — copied from StravaSquad/predict_pb.py.

Three explainable methods:
  - Riegel power-law scaling
  - VDOT (Jack Daniels) VO2max-based inversion
  - Elevation-adjusted Riegel (flat-pace correction)
"""
import numpy as np
from scipy.optimize import brentq

ELEVATION_FACTOR_SEC_PER_M_PER_KM = 2.0


def riegel(t1_sec, d1_m, d2_m, exponent=1.06):
    return t1_sec * (d2_m / d1_m) ** exponent


def _vo2_cost(velocity_m_per_min):
    return -4.60 + 0.182258 * velocity_m_per_min + 0.000104 * velocity_m_per_min ** 2


def _vo2_pct(time_min):
    return 0.8 + 0.1894393 * np.exp(-0.012778 * time_min) + 0.2989558 * np.exp(-0.1932605 * time_min)


def compute_vdot(distance_m, time_sec):
    time_min = time_sec / 60.0
    velocity = distance_m / time_min
    return _vo2_cost(velocity) / _vo2_pct(time_min)


def vdot_time(vdot, distance_m):
    def f(time_min):
        velocity = distance_m / time_min
        return _vo2_cost(velocity) / _vo2_pct(time_min) - vdot
    time_min = brentq(f, 1.0, 600.0)
    return time_min * 60.0


def elevation_adjusted_riegel(t1_sec, d1_m, source_elev_per_km, d2_m, target_elev_per_km):
    source_pace_sec_km = t1_sec / (d1_m / 1000.0)
    flat_pace = source_pace_sec_km - ELEVATION_FACTOR_SEC_PER_M_PER_KM * source_elev_per_km
    flat_t1 = flat_pace * (d1_m / 1000.0)
    flat_t2 = riegel(flat_t1, d1_m, d2_m)
    flat_pace2 = flat_t2 / (d2_m / 1000.0)
    target_pace2 = flat_pace2 + ELEVATION_FACTOR_SEC_PER_M_PER_KM * target_elev_per_km
    return target_pace2 * (d2_m / 1000.0)
