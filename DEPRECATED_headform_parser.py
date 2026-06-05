df = pd.read_csv(csv_file, skiprows=22)
df.columns = df.columns.str.strip()
df['Time'] = (df['Time'] * 1e9).astype(int)

#iso naming
rename_map = {
    'Chan 0:6DX0855-AV1': 'gyroZ_dps',
    'Chan 1:6DX0855-AV2': 'gyroY_dps',
    'Chan 2:6DX0855-AV3': 'gyroX_dps',
    'Chan 3:6DX0855-AC1': 'accelZ_g',
    'Chan 4:6DX0855-AC2': 'accelY_g',
    'Chan 5:6DX0855-AC3': 'accelX_g',
    'Time': 'time_ns'
}
df = df.rename(columns=rename_map)

df['accelMag_g'] = np.sqrt(df['accelX_g']**2 + df['accelY_g']**2 + df['accelZ_g']**2)
df['gyroMag_dps'] = np.sqrt(df['gyroX_dps']**2 + df['gyroY_dps']**2 + df['gyroZ_dps']**2)

df = frame_data(df)
df_downsampled = df.iloc[::downsample, :]
df_downsampled.to_parquet(pq_file, engine='pyarrow', index=False, compression='snappy')

