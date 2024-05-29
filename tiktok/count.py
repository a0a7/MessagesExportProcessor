import os
import json
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Config
user_timezone = 'America/Chicago'
# When does a new day start? Default 04:00
day_start_hour = 4

def parse_json_file(file_path):
    timestamps_chat_sender = {}

    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        direct_messages = data.get('Direct Messages', {})
        chat_history = direct_messages.get('Chat History', {}).get('ChatHistory', {})
        
        for chat_title, messages in chat_history.items():
            chat_name = chat_title.strip().replace("Chat History with ", "").replace(":", "")
            timestamps_chat_sender[chat_name] = {}
            
            for message in messages:
                sender = message['From']
                timestamp = message['Date']
                timestamp_dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
                timestamp_ms = int(timestamp_dt.timestamp() * 1000)
                
                if sender not in timestamps_chat_sender[chat_name]:
                    timestamps_chat_sender[chat_name][sender] = []
                timestamps_chat_sender[chat_name][sender].append(timestamp_ms)
    
    return timestamps_chat_sender

def count_messages_per_day(timestamps, timezone):
    tz = pytz.timezone(timezone)
    date_times = [datetime.fromtimestamp(timestamp / 1000, tz) - timedelta(hours=day_start_hour)  for timestamp in timestamps]
    
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
    timestamps_chat_sender = parse_json_file('tiktok/user_data.json')
    
    for chat_name, sender_timestamps in timestamps_chat_sender.items():
        chat_dir = os.path.join('tiktok/output', chat_name)
        os.makedirs(chat_dir, exist_ok=True)
        
        for sender, timestamps in sender_timestamps.items():
            counts = count_messages_per_day(timestamps, user_timezone)
            
            output_csv = os.path.join(chat_dir, f'{sender}.csv')
            
            save_counts_to_csv(counts, output_csv)
            
            print(f'Done w {sender} in chat "{chat_name}"')