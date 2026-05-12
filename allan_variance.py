import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path

def calculate_allan_variance(data, dt):
    """
    Calculates the overlapping Allan variance of a signal.
    
    Args:
        data: The sensor data (rate signal, e.g., rad/s or m/s^2).
        dt: The sampling interval in seconds.
        
    Returns:
        taus: Array of tau values (observation times).
        sigmas: Array of Allan deviations (square root of Allan variance).
    """
    N = len(data)
    # theta is the integrated signal (e.g., angle if input is angular rate)
    theta = np.cumsum(data) * dt
    
    # We want tau to range from dt up to about N/4 * dt
    # Using log-spaced values for m (number of samples per block)
    m_max = N // 4
    if m_max < 1:
        return np.array([]), np.array([])
        
    # Generate log-spaced m values
    ms = np.unique(np.logspace(0, np.log10(m_max), 100).astype(int))
    ms = ms[ms > 0]
    
    taus = ms * dt
    sigmas = []
    
    for m in ms:
        # Overlapping Allan Variance formula:
        # sigma^2(m*dt) = 1/(2*(m*dt)^2 * (N-2m)) * sum_{i=0}^{N-2m-1} (theta[i+2m] - 2*theta[i+m] + theta[i])^2
        # Vectorized implementation:
        diffs = theta[2*m:] - 2*theta[m:-m] + theta[:-2*m]
        av = np.sum(diffs**2) / (2 * (m*dt)**2 * (N - 2*m))
        sigmas.append(np.sqrt(av))
        
    return taus, np.array(sigmas)

def process_allan_variance():
    input_dir = Path("stationary_ignore/framed")
    output_data_dir = Path("stationary_ignore/allan_variance")
    output_img_dir = Path("stationary_ignore/allan_variance_images")
    
    output_data_dir.mkdir(parents=True, exist_ok=True)
    output_img_dir.mkdir(parents=True, exist_ok=True)
    
    files = list(input_dir.glob("*.csv"))
    if not files:
        print(f"No files found in {input_dir}")
        return
    
    for f in files:
        print(f"Processing {f.name}...")
        try:
            df = pd.read_csv(f)
            
            # Determine columns to analyze based on file prefix
            if f.name.startswith("accel"):
                cols = ["LinAccX (m/s2)", "LinAccY (m/s2)", "LinAccZ (m/s2)", "LinAccRes (m/s2)"]
            elif f.name.startswith("gyro"):
                cols = ["RotVelX (rad/s)", "RotVelY (rad/s)", "RotVelZ (rad/s)", "RotVelRes (rad/s)"]
            else:
                # Default to all numeric columns except Time, triggered, and battery info
                cols = [c for c in df.columns if any(x in c for x in ["Acc", "Vel", "Rot"])]
            
            # Filtering existing columns
            cols = [c for c in cols if c in df.columns]
            
            if not cols:
                print(f"Skipping {f.name}: No relevant columns found.")
                continue
                
            # Sampling interval
            dt = df["Time (s)"].diff().median()
            if pd.isna(dt) or dt <= 0:
                # Try to infer from first two points if median fails
                if len(df) > 1:
                    dt = df["Time (s)"].iloc[1] - df["Time (s)"].iloc[0]
                
                if pd.isna(dt) or dt <= 0:
                    print(f"Skipping {f.name}: Invalid sampling interval (dt={dt}).")
                    continue
            
            plt.figure(figsize=(10, 7))
            all_results = {}
            
            max_taus = []
            
            for col in cols:
                data = df[col].values
                taus, sigmas = calculate_allan_variance(data, dt)
                
                if len(taus) > 0:
                    all_results[f"{col}_sigma"] = sigmas
                    if len(max_taus) < len(taus):
                        max_taus = taus
                    
                    plt.loglog(taus, sigmas, label=col)
            
            if not all_results:
                print(f"No results for {f.name}")
                plt.close()
                continue
                
            # Save CSV data
            # Ensure all sigmas are aligned with the longest tau array if they differ
            # (In this case they should be identical as they share the same N and dt)
            res_df = pd.DataFrame({"tau_s": max_taus})
            for col_name, sigmas in all_results.items():
                res_df[col_name] = sigmas
            
            data_out_path = output_data_dir / f.name.replace(".csv", "_allan.csv")
            res_df.to_csv(data_out_path, index=False)
            
            # Finalize and save plot
            plt.title(f"Allan Deviation Analysis - {f.stem}")
            plt.xlabel("Tau (s)")
            plt.ylabel("Allan Deviation (Unit/s or Unit)")
            plt.legend()
            plt.grid(True, which="both", ls="-", alpha=0.5)
            
            img_out_path = output_img_dir / f.name.replace(".csv", ".svg")
            plt.savefig(img_out_path, format='svg')
            plt.close()
            
            print(f"Successfully processed {f.name}")
            
        except Exception as e:
            print(f"Error processing {f.name}: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    process_allan_variance()
