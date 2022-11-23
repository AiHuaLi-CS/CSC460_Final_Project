# Imports
import requests
import pandas as pd
import datetime

# Data Loading
def get_data(ticker, days):
    present = datetime.datetime.now().date()
    past = present - datetime.timedelta(days = days)
    print("Trades from {} to {}".format(past, present))
    
    # Gets url response
    res = requests.get("https://api.polygon.io/v2/aggs/ticker/{}/range/1/hour/{}/{}?adjusted=true&sort=asc&limit=50000&apiKey=yuasIoFiphAuTZl8p0_ohMDGYCIzpzVF".format(ticker, past, present))

    # Parses the text response in dataframe
    df = pd.read_json(res.text)
    
    return df
    
# Data Cleaning
def clean_df(df):
    # Gets total length of the data
    DF_LENGTH = len(df)
    
    # Parses the [results] column into a seperate dataframe 
    df_aug = pd.DataFrame([df["results"][0]])
    
    for i in range(1, DF_LENGTH):
        df_curr = pd.DataFrame([df["results"][i]])
        df_aug = df_aug.append(df_curr, ignore_index=True)
        
    # drops the [vw], [n] columns
    df_dropped = df_aug.drop(['vw','n'],axis=1)
    
    # Renames all columns in place
    df_dropped.rename(columns = {'v':'Volumn', 'o':'Open', 'c':'Close', 'h':'Highest', 'l':'Lowest', 't':'Timestamp',}, inplace = True)

    # Converts Unix timestamp to Datetime
    df_dropped['Timestamp'] = pd.to_datetime(df_dropped['Timestamp'],unit='ms')
    
    # Adds column [Hour]
    df_dropped['Hour'] = df_dropped.Timestamp.dt.hour
    
    # Gets info for regular market hours only (from 9 a.m. to 4 p.m. ET.)
    df_dropped = df_dropped[df_dropped['Hour'].between(9, 16)]
    
    # Adds column [Profit]
    df_dropped['Profit'] = df_dropped.apply(lambda row: row['Close'] - row['Open'], axis=1)
    
    return df_dropped
    
def main():
    ticker = 'SPY'
    days = 90
    df = get_data(ticker, days)
    df = clean_df(df)
    print(df)
    #df.to_csv('folder/subfolder/cleaned_data.csv')

if __name__ == "__main__":
    main()
