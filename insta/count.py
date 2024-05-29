import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Config
user_timezone = 'America/Chicago'
# When does a new day start? Default 04:00
day_start_hour = 4

def parse_json_files():
    timestamps_by_sender = {}

    for filename in os.listdir('insta/messages'):
        if filename.endswith('.json'):
            file_path = os.path.join('insta/messages/', filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
                messages = data.get('messages', [])
                print(f'Parsing {len(messages)} messages from {filename}')
                for message in messages:
                    sender = message['sender_name']
                    timestamp = message['timestamp_ms']
                    
                    if sender not in timestamps_by_sender:
                        timestamps_by_sender[sender] = []
                    timestamps_by_sender[sender].append(timestamp)
    
    return timestamps_by_sender

def count_messages_per_day(timestamps, timezone):
    tz = pytz.timezone(timezone)
    date_times = [datetime.fromtimestamp(timestamp / 1000, tz) - timedelta(hours=day_start_hour) for timestamp in timestamps]
    
    df = pd.DataFrame(date_times, columns=['DateTime'])
    df['Date'] = df['DateTime'].dt.date
    
    daily_counts = df.groupby('Date').size().reset_index(name='Count')
    
    if not daily_counts.empty:
        date_range = pd.date_range(start=daily_counts['Date'].min(), end=daily_counts['Date'].max())
        daily_counts = daily_counts.set_index('Date').reindex(date_range, fill_value=0).rename_axis('Date').reset_index()
    
    return daily_counts

def save_counts_to_csv(counts, output_csv):
    counts.to_csv(output_csv, index=False)

if __name__ == '__main__':
    timestamps_by_sender = parse_json_files()
    
    for sender, timestamps in timestamps_by_sender.items():
        counts = count_messages_per_day(timestamps, user_timezone)
                
        save_counts_to_csv(counts, f'insta/output/{sender}.csv')
        
        print(f'Done w {sender}')