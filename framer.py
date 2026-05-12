import pandas as pd
import numpy as np
from scipy import signal
from pathlib import Path

def frame_2_ref(df_phone, df_ref, p_col="RotVelRes (rad/s)", r_col="RotVelRes (rad/s)"):
    """
    Finds the lag between df_phone and df_ref using cross-correlation.
    """
    sig_p = np.nan_to_num(df_phone[p_col].values)
    sig_r = np.nan_to_num(df_ref[r_col].values)
    
    sig_p = (sig_p - np.mean(sig_p)) / (np.std(sig_p) + 1e-9)
    sig_r = (sig_r - np.mean(sig_r)) / (np.std(sig_r) + 1e-9)
    
    corr = signal.correlate(sig_p, sig_r, mode='full', method='fft')
    lags = signal.correlation_lags(len(sig_p), len(sig_r), mode='full')
    
    return lags[np.argmax(corr)]

def frame_phone_drop():
    phone_dir = Path("phone_drop_test_data_ignore/phone_cleaned")
    ref_dir = Path("phone_drop_test_data_ignore/phone_reference_signals")
    output_dir = Path("phone_drop_test_data_ignore/phone_framed")
    log_csv = Path("test_log_ignore/framing_logs.csv")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = []
    phone_files = list(phone_dir.glob("*.csv"))
    
    for phone_file in phone_files:
        parts = phone_file.stem.split("_")
        if len(parts) < 4: continue
            
        speed, config, repeat, phone_id = parts[0], parts[1], parts[2], parts[3]
        
        # Construct reference name
        ref_name = f"{speed}_{config}_{repeat}_Headform_Unfiltered_Transformed_{phone_id}_REF.csv"
        ref_path = ref_dir / ref_name
        
        if not ref_path.exists():
            print(f"Skipping {phone_file.name}: Reference {ref_name} not found.")
            continue
        
        try:
            df_p = pd.read_csv(phone_file)
            df_r = pd.read_csv(ref_path)
            
            lag_idx = frame_2_ref(df_p, df_r)
            
            # Time alignment
            t0_r = df_r["Time (s)"].iloc[0]
            t_at_lag = df_p["Time (s)"].iloc[max(0, min(lag_idx, len(df_p)-1))]
            offset = t0_r - t_at_lag
            
            # Crop
            df_framed = df_p.iloc[max(0, lag_idx) : min(len(df_p), lag_idx + len(df_r))].copy()
            df_framed["Time (s)"] += offset
            
            # Save
            out_name = phone_file.name.replace("_cleaned.csv", "_framed.csv")
            df_framed.to_csv(output_dir / out_name, index=False)
            
            results.append({
                "speed": speed, "config": config, "repeat": repeat, "phone_id": phone_id,
                "phone_file": phone_file.name, "ref_file": ref_name, "framed_file": out_name,
                "lag": lag_idx, "offset": offset
            })
            print(f"Processed {phone_file.name}")
            
        except Exception as e:
            print(f"Error processing {phone_file.name}: {e}")
            
    if results:
        pd.DataFrame(results).to_csv(log_csv, index=False)
        print(f"Logs saved to {log_csv}")

def frame_stationary():
    phone_dir = Path("stationary_ignore/cleaned")
    output_dir = Path("stationary_ignore/framed")
    log_csv = Path("test_log_ignore/stationary_framing_logs.csv")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_dir.mkdir(parents=True, exist_ok=True)
    
    results = []
    phone_files = list(phone_dir.glob("*.csv"))
    
    for phone_file in phone_files:
        parts = phone_file.stem.split("_")
        if len(parts) < 4: continue
            
        sensor, _, date, time, phone_id = parts[0], parts[1], parts[2], parts[3],parts[4]
        
        # Construct reference name
        output_name = f"{sensor}_stationary_{date}_{time}_{phone_id}.csv"
        output_path = output_dir / output_name
        
        try:
            df = pd.read_csv(phone_file)

            t0_r = df["Time (s)"].iloc[0]
            t1_r = df["Time (s)"].iloc[-1]
            clean = 900 #15min
            
            df_framed = df[
                (df["Time (s)"] > t0_r + clean) &
                (df["Time (s)"] < t1_r - clean)
            ].copy()

            df_framed["Time (s)"] -= df_framed["Time (s)"].iloc[0]
            
            df_framed.to_csv(output_path, index=False)
            
            results.append({
                "sensor":sensor ,"date": date, "time": time, "phone_id": phone_id,
                "phone_file": phone_file.name, "framed_file": output_name,
            })
            print(f"Processed {phone_file.name}")
            
        except Exception as e:
            print(f"Error processing {phone_file.name}: {e}")
            
    if results:
        pd.DataFrame(results).to_csv(log_csv, index=False)
        print(f"Logs saved to {log_csv}")


if __name__ == "__main__":
    frame_stationary()
    #frame_phone_drop()
