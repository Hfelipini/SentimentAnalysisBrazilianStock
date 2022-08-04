#Used Modules
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import datetime as dt
from datetime import date
import numpy as np
from pandas_datareader import data as web

# Connect to the Postgres Database
conn = psycopg2.connect("host=127.0.0.1 port=7000 dbname=postgres user=postgres password=example")
print(conn)
cursor = conn.cursor()

# Query and create the desirable Dataframe with the Days and Average Polarity from each one from Twitter
df = pd.read_sql_query('SELECT bet_days, avg(first) FROM ( SELECT user_id, bet_days, avg(polarity) as first FROM Tweet GROUP BY user_id, bet_days) as inner_query GROUP BY bet_days ORDER BY bet_days DESC LIMIT 365',conn)

# Get the correct date through the same operation done in the Python - Sentiment Analysis - V3
df['StartTime'] = date(2000,1,1)                                                                        
df['Calendar'] = df['StartTime'] + df['bet_days'].map(dt.timedelta)                                     # Get the correct day from each value to use as X label in the graphs

# Same function from Python - Sentiment Analysis - V3
def getDays(date1):                                                                                     
    return int(str(date(date1.year,date1.month,date1.day)-date(2000,1,1))[0:4])                         # Return the calenday days between the start date and the actual date provided

# Define the Yahoo Dataframe, getting the Ibovespa information using the data provided from our query above
# Using Extract and Transform operations to get the Dataframe ready to search, compare and find
df2 = web.DataReader(f'^BVSP', data_source='yahoo', start=df.iloc[len(df)-1][3], end=df.iloc[0][3])     # Extract the infos from Yahoo
df2['Reta'] = df2['Close'].shift(1)                                                                     # Use the shift function to calculate the return through - (Rt - Rt-1)/Rt-1
df2['Rent'] = ((df2['Close']-df2['Reta'])/df2['Reta'])*100                                              # Get the Daily Return from Ibov
df2['Date2'] = df2.index                                                                                # Get the calendar index to a column
df2 = df2.sort_values(by=['Date2'], ascending=False)                                                    # Sort the Dataframe usin the Date2 from the newest to the oldest
df2['GetDays'] = df2['Date2'].apply(getDays)                                                            # Create a GetDays Column, same as first Dataframe, to search and find the correct values
df2['Row'] = np.arange(len(df2))                                                                        # Add a column to get the correct Row, so it can be used in the for loop below 

# Search and find values through the 'GetDays' column from both dataframes.
# If it finds the value, simply substitute the value from 0 to the correct value from the Yahoo Dataframe
# Here it's used the Error Handling due to the lenght of dataframes
# The Yahoo dataframe only get the workdays meanwhile the Twitter still works on weekends

for row in range(len(df)):
  try:
    row_identified = df2[df2['GetDays'].isin([df.iloc[row][0]])].iloc[0][10]                             # Identify the row in the Yahoo Dataframe that contains the Return in the specified date from the first DataFrame 
    df.at[row,'Rent']=df2.iloc[row_identified][7]                                                        # Assign the correct value of Ibovespa Daily Return to the first DataFrame
    df.at[row,'Close']=df2.iloc[row_identified][3]  
  except IndexError:
    pass

# Code to fill the NaN values from the weekend and holidays and the oldest value that can't calculated
df.at[len(df)-1,'Rent']=0
df.at[len(df)-1,'Close']=122516
df['Rent'].fillna(method='backfill', inplace=True)
df['Close'].fillna(method='backfill', inplace=True)

# Create figure and axis objects with subplots()
fig,ax = plt.subplots()
ax.plot(df['Calendar'], df['avg'].rolling(7).mean(),color="red", marker=",")   
ax.plot(df['Calendar'], df['Rent'].rolling(7).mean(),color="green", marker=",")                            # Make a plot with a Moving Average of 3 days
ax.set_xlabel("Ibovespa Daily Return", color="green", fontsize = 14)                                                                      # Set x-axis label
ax.set_ylabel("Polarity Average",color="red",fontsize=14)                                                # Set y-axis label
ax2=ax.twinx()                                                                                           # Twin object for two different y-axis on the sample plot
#ax2.plot(df['Calendar'], df['Rent'].rolling(3).mean(),color="blue",marker=",")                           # Make a plot with different y-axis using second axis object
ax2.plot(df['Calendar'], df['Close'],color="blue",marker=",")                           # Make a plot with different y-axis using second axis object
ax2.set_ylabel("Ibovespa Close Value",color="blue",fontsize=14)
plt.show()
fig.savefig('Result_Polarity_Return.jpg',format='jpeg',dpi=100,bbox_inches='tight')                      # save the plot as a file
df.to_csv("Dataframe.csv")                                                                               # Export as CSV

#Calculate the Correlation between Average Polarity from Tweets with the Ibovespa Daily Return

print(df2)
print(df)
data = df[['avg','Rent']]
correlation = data.corr(method='pearson')
print(correlation)

