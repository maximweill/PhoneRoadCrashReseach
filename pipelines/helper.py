import numpy as np

# =========================================================
# ALLAN VARIANCE HELPERS
# =========================================================

def _compute_allan_variance(data: np.ndarray, dt: float) -> tuple[np.ndarray, np.ndarray]:
    """
    Calculates the overlapping Allan variance of a signal.
    """
    N = len(data)
    # theta is the integrated signal (e.g., angle if input is angular rate)
    theta = np.cumsum(data) * dt

    # We want tau to range from dt up to about N/4 * dt
    m_max = N // 4
    if m_max < 1:
        return np.array([]), np.array([])

    # Generate log-spaced m values (number of samples per block)
    ms = np.unique(np.logspace(0, np.log10(m_max), 100).astype(int))
    ms = ms[ms > 0]

    taus = ms * dt
    sigmas = []

    for m in ms:
        # Overlapping Allan Variance formula:
        # sigma^2(m*dt) = 1/(2*(m*dt)^2 * (N-2m)) * sum_{i=0}^{N-2m-1} (theta[i+2m] - 2*theta[i+m] + theta[i])^2
        diffs = theta[2*m:] - 2*theta[m:-m] + theta[:-2*m]
        av = np.sum(diffs**2) / (2 * (m*dt)**2 * (N - 2*m))
        sigmas.append(np.sqrt(av))

    return taus, np.array(sigmas)

# =========================================================
# PSD HELPERS
# =========================================================

def _compute_psd(data: np.ndarray, fs: float) -> tuple[np.ndarray, np.ndarray]:
    """
    Calculates the Power Spectral Density of a signal using Welch's method.
    """
    from scipy.signal import welch
    
    # nperseg should be chosen based on the length of the data and desired frequency resolution
    # A common default or 1024/2048 is often used, but we can also use a fraction of the data length.
    nperseg = min(len(data), 2048)
    freqs, psd = welch(data, fs=fs, nperseg=nperseg)
    
    return freqs, psd

# =========================================================
# CONVENTION HELPERS
# =========================================================

def naming_convention(phone_id:str,config:str,target_speed_mps:str,repeat:str,is_reference = False):
    if is_reference:
        return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_Headform_Transformed_{phone_id}"
    return f"{target_speed_mps}mps_{config}_REPEAT{repeat}_{phone_id}"
