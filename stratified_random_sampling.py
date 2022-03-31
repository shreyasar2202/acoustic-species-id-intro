import pandas as pd
from datetime import datetime
def stratified_sampling(file_path):
    
    # Reading the CSV file
    df = pd.read_csv(file_path)

    # Consider only the rows with no errors
    df = df[df['Error'].isna()]

    # Consider the rows where the duration is a minute long
    df = df[(df['Duration'].notna()) & (df['Duration'] >= 60.0) & (df['Duration'] < 61.0)]

    # Consider the rows where the clips are 46 megabytes and under 47 megabytes
    df = df[(df['FileSize'].notna()) & (df['FileSize'] > 46 * 10 ** 6)\
             & (df['FileSize'] < 47 * 10 ** 6)]

    # Consider the rows where comments are not NA
    df = df[df['Comment'].notna()]
    
    # Create a column called hour by extracting it from the comments
    df['hour'] = df['Comment'].apply(lambda x: datetime.strptime(' '.join(x.split()[2:4]),\
                '%H:%M:%S %d/%m/%Y')).dt.hour.tolist()
    
    # Group by the Audio Moth Code and hour and sample one row from each group
    df = df.groupby(["AudioMothCode", "hour"]).apply(lambda x: x.sample(1))

    # Consider the Audio Moths have one sample at each hour
    counts = df["AudioMothCode"].value_counts()
    df = df[df['AudioMothCode'].isin(counts[counts == 24].index)]

    # After all this processing, if the dataframe has non-zero size, store it as csv and return true
    # Else, return false
    if(df.size > 0):
        df.to_csv('stratified_samples.csv', index=False)
        return True

    return False
    
print(stratified_sampling('Peru_2019_AudioMoth_Data_Full.csv'))
