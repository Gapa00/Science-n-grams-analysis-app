# File: app/burst_detection/burst_algorithm.py
"""
Optimized Kleinberg burst detection algorithm
Fast, numba-accelerated implementation with proper Viterbi algorithm
"""

import pandas as pd
import numpy as np
import math
from numba import jit

@jit(nopython=True, fastmath=True)
def tau(i1, i2, gamma, n):
    """Transition cost function"""
    if i1 >= i2:
        return 0.0
    else: 
        return (i2 - i1) * gamma * math.log(n)

@jit(nopython=True, fastmath=True)
def fit(d, r, p):
    """Fit cost function using binomial log-likelihood"""
    # Ensure we don't have impossible cases
    if r > d or p <= 0 or p >= 1 or d <= 0:
        return 1e6
    
    log_binom = math.lgamma(d + 1) - math.lgamma(r + 1) - math.lgamma(d - r + 1)
    log_likelihood = log_binom + r * math.log(p) + (d - r) * math.log(1 - p)
    result = -log_likelihood
    
    return result if math.isfinite(result) else 1e6

def burst_detection(r, d, n, s=2, gamma=1.0, smooth_win=1):
    """
    Corrected Kleinberg burst detection with proper Viterbi algorithm
    
    Parameters:
    -----------
    r : array-like
        Number of target events in each time period
    d : array-like  
        Total number of events in each time period
    n : int
        Number of time periods
    s : float
        Multiplicative distance between states (default: 2)
    gamma : float
        Difficulty to move up a state (default: 1.0)
    smooth_win : int
        Smoothing window width (default: 1, no smoothing)
    """
    
    k = 2  # Two states: baseline (0) and burst (1)
    
    # Convert to numpy arrays for efficiency
    r = np.asarray(r, dtype=np.float64)
    d = np.asarray(d, dtype=np.float64)
    
    # Smooth the data if requested
    if smooth_win > 1:
        # Use numpy convolution for efficient smoothing
        kernel = np.ones(smooth_win) / smooth_win
        temp_p = r / np.maximum(d, 1e-10)  # Avoid division by zero
        
        # Pad for convolution
        temp_p_padded = np.pad(temp_p, (smooth_win//2, smooth_win//2), mode='edge')
        temp_p_smooth = np.convolve(temp_p_padded, kernel, mode='valid')[:n]
        
        r = temp_p_smooth * d
        valid_mask = ~np.isnan(r) & np.isfinite(r)
        real_n = np.sum(valid_mask)
    else:
        valid_mask = np.ones(n, dtype=bool)
        real_n = n
    
    # Calculate expected proportions
    p = {}
    valid_r = r[valid_mask]
    valid_d = d[valid_mask]
    
    total_target = np.sum(valid_r)
    total_events = np.sum(valid_d)
    
    if total_events == 0:
        return np.full(n, np.nan), d, r, {0: 0, 1: 0}, np.full((n, k), -np.inf)
    
    p[0] = total_target / total_events  # Baseline proportion
    p[1] = min(p[0] * s, 0.99999)      # Burst proportion (capped at < 1)
    
    # Use optimized Viterbi implementation
    delta, psi = _viterbi_forward_optimized(r, d, p[0], p[1], gamma, n, valid_mask, real_n, smooth_win)
    
    # Backward pass - trace optimal path
    q = np.full(n, np.nan)
    
    # Find best final state
    start_idx = 0 if smooth_win <= 1 else (smooth_win - 1) // 2
    last_valid_t = start_idx + real_n - 1
    
    if last_valid_t < n and last_valid_t >= 0:
        q[last_valid_t] = np.argmax(delta[last_valid_t, :])
        
        # Trace back optimal path
        for t in range(last_valid_t - 1, start_idx - 1, -1):
            if valid_mask[t] and t + 1 < n:
                q[t] = psi[t + 1, int(q[t + 1])]
    
    return q, d, r, p, delta

@jit(nopython=True, fastmath=True)
def _viterbi_forward_optimized(r, d, p0, p1, gamma, n, valid_mask, real_n, smooth_win):
    """Optimized Viterbi forward pass"""
    k = 2
    
    # Pre-allocate matrices
    delta = np.full((n, k), -np.inf, dtype=np.float64)
    psi = np.zeros((n, k), dtype=np.int32)
    
    # Precompute transition costs
    tau_00 = tau(0, 0, gamma, real_n)
    tau_01 = tau(0, 1, gamma, real_n)
    tau_10 = tau(1, 0, gamma, real_n)
    tau_11 = tau(1, 1, gamma, real_n)
    
    # Initialize first time step
    start_idx = 0 if smooth_win <= 1 else (smooth_win - 1) // 2
    
    if start_idx < n and valid_mask[start_idx]:
        delta[start_idx, 0] = -fit(d[start_idx], r[start_idx], p0)
        delta[start_idx, 1] = -fit(d[start_idx], r[start_idx], p1)
    
    # Forward pass
    for t in range(start_idx + 1, min(start_idx + real_n, n)):
        if not valid_mask[t]:
            continue
        
        # Precompute fit costs for current time step
        fit_cost_0 = fit(d[t], r[t], p0)
        fit_cost_1 = fit(d[t], r[t], p1)
        
        # State 0 (baseline)
        if delta[t-1, 0] != -np.inf:
            cost_0_from_0 = -delta[t-1, 0] + tau_00 + fit_cost_0
        else:
            cost_0_from_0 = np.inf
            
        if delta[t-1, 1] != -np.inf:
            cost_0_from_1 = -delta[t-1, 1] + tau_10 + fit_cost_0
        else:
            cost_0_from_1 = np.inf
        
        if cost_0_from_0 <= cost_0_from_1:
            delta[t, 0] = -cost_0_from_0
            psi[t, 0] = 0
        else:
            delta[t, 0] = -cost_0_from_1
            psi[t, 0] = 1
        
        # State 1 (burst)
        if delta[t-1, 0] != -np.inf:
            cost_1_from_0 = -delta[t-1, 0] + tau_01 + fit_cost_1
        else:
            cost_1_from_0 = np.inf
            
        if delta[t-1, 1] != -np.inf:
            cost_1_from_1 = -delta[t-1, 1] + tau_11 + fit_cost_1
        else:
            cost_1_from_1 = np.inf
        
        if cost_1_from_0 <= cost_1_from_1:
            delta[t, 1] = -cost_1_from_0
            psi[t, 1] = 0
        else:
            delta[t, 1] = -cost_1_from_1
            psi[t, 1] = 1
    
    return delta, psi

def enumerate_bursts(q, label="burst"):
    """Enumerate burst periods from state sequence"""
    bursts = pd.DataFrame(columns=['label', 'begin', 'end', 'weight'])
    
    if len(q) == 0 or np.all(np.isnan(q)):
        return bursts
    
    # Remove NaN values for processing
    valid_indices = ~np.isnan(q)
    if not np.any(valid_indices):
        return bursts
    
    q_clean = q[valid_indices]
    indices = np.where(valid_indices)[0]
    
    # Vectorized burst detection using diff
    if len(q_clean) <= 1:
        return bursts
    
    state_changes = np.diff(q_clean)
    
    # Find burst starts and ends
    burst_starts_rel = np.where(state_changes > 0)[0] + 1  # +1 because diff shifts indices
    burst_ends_rel = np.where(state_changes < 0)[0]
    
    bursts_data = []
    b = 0
    
    # Handle case where we start in a burst
    if len(burst_ends_rel) > 0 and (len(burst_starts_rel) == 0 or burst_ends_rel[0] < burst_starts_rel[0]):
        bursts_data.append({
            'label': label,
            'begin': indices[0],
            'end': indices[burst_ends_rel[0]],
            'weight': 0
        })
        burst_ends_rel = burst_ends_rel[1:]
        b += 1
    
    # Match starts and ends
    min_len = min(len(burst_starts_rel), len(burst_ends_rel))
    for i in range(min_len):
        bursts_data.append({
            'label': label,
            'begin': indices[burst_starts_rel[i]],
            'end': indices[burst_ends_rel[i]],
            'weight': 0
        })
        b += 1
    
    # Handle case where burst continues to end
    if len(burst_starts_rel) > len(burst_ends_rel):
        bursts_data.append({
            'label': label,
            'begin': indices[burst_starts_rel[-1]],
            'end': indices[-1],
            'weight': 0
        })
    
    if bursts_data:
        bursts = pd.DataFrame(bursts_data)
    
    return bursts

def burst_weights(bursts, r, d, p):
    """Calculate burst weights based on cost differences"""
    if len(bursts) == 0:
        return bursts
    
    # Convert to numpy arrays for efficiency
    r = np.asarray(r, dtype=np.float64)
    d = np.asarray(d, dtype=np.float64)
    
    # Vectorized weight calculation
    weights = np.zeros(len(bursts))
    
    for b in range(len(bursts)):
        begin_idx = int(bursts.iloc[b]['begin'])
        end_idx = int(bursts.iloc[b]['end'])
        
        # Ensure valid indices
        begin_idx = max(0, min(begin_idx, len(r) - 1))
        end_idx = max(begin_idx, min(end_idx, len(r) - 1))
        
        # Vectorized cost difference calculation
        r_slice = r[begin_idx:end_idx + 1]
        d_slice = d[begin_idx:end_idx + 1]
        
        # Calculate cost differences for the slice
        cost_diff_sum = 0.0
        for t in range(len(r_slice)):
            cost_diff = fit(d_slice[t], r_slice[t], p[0]) - fit(d_slice[t], r_slice[t], p[1])
            cost_diff_sum += cost_diff
        
        weights[b] = cost_diff_sum
    
    # Update bursts DataFrame
    bursts = bursts.copy()
    bursts['weight'] = weights
    
    return bursts.sort_values(by='weight', ascending=False)