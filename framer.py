import pandas as pd
import numpy as np
from scipy import signal
from pathlib import Path

def frame_2_ref(df, reference, column="gyroMag_dps"):
    """
    Finds the lag between df and reference using cross-correlation.
    Returns the lag in terms of indices.
    """
    # Normalize signals
    sig1 = df[column].values
    sig2 = reference[column].values
    
    # Handle potential NaNs from resampling or normalization
    sig1 = np.nan_to_num(sig1)
    sig2 = np.nan_to_num(sig2)
    
    sig1 = (sig1 - np.mean(sig1)) / (np.std(sig1) + 1e-9)
    sig2 = (sig2 - np.mean(sig2)) / (np.std(sig2) + 1e-9)
    
    # Cross-correlation using FFT
    corr = signal.correlate(sig1, sig2, mode='full', method='fft')
    lags = signal.correlation_lags(len(sig1), len(sig2), mode='full')
    
    best_lag = lags[np.argmax(corr)]
    
    return best_lag

def main():
    # UPDATED: Points to cleaned phone data and CSV reference signals
    phone_dir = Path("phone_drop_test_data_ignore/phone_cleaned")
    ref_dir = Path("phone_drop_test_data_ignore/phone_reference_signals")
    output_csv = Path("test_log_ignore/phone_cropping_params.csv")
    
    results = []
    
    phone_files = list(phone_dir.glob("*.csv"))
    
    for phone_file in phone_files:
        # UPDATED: Reference files are now CSV
        ref_file = ref_dir / f"{phone_file.stem}_REF.csv"
        
        if not ref_file.exists():
            print(f"Skipping {phone_file.name}: Reference file not found.")
            continue
            
        print(f"Processing {phone_file.name}...")
        
        try:
            df_phone = pd.read_csv(phone_file)
            df_ref = pd.read_csv(ref_file)
            
            # Use gyroMag
            col = "gyroMag_dps"
            lag_idx = frame_2_ref(df_phone, df_ref, column=col)
            
            # Calculate time offset based on phone's sampling rate
            time_col = "time_ns"
            dts = np.diff(df_phone[time_col].values)
            avg_dt = np.median(dts)
            q1,q3 = np.quantile(dts, [0.25, 0.75])

            t0_ref = df_ref[time_col].iloc[0]

            # Correct lag_ns: The timestamp in df_phone at lag_idx should align with t0_ref.
            # df_phone[time_col].iloc[lag_idx] + lag_ns = t0_ref
            lag_ns = t0_ref - df_phone[time_col].iloc[lag_idx]

            results.append({
                "phone_file": phone_file.name,
                "ref_file": ref_file.name,
                "lag_indices": lag_idx,
                "ref_length": len(df_ref),
                "lag_ns": lag_ns,
                "avg_dt": avg_dt,
                "IQR":q3-q1,
            })
            
        except Exception as e:
            print(f"Error processing {phone_file.name}: {e}")
            
    if results:
        pd.DataFrame(results).to_csv(output_csv, index=False)
        print(f"Cropping parameters saved to {output_csv}")
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()
