import xmltodict
import pandas as pd
from datetime import datetime, timedelta
import pytz

# Config
numbers_to_match = ['6123688206', '+16123688206'] 
user_timezone = 'America/Chicago'
# When does a new day start? Default 04:00
day_start_hour = 4

def parse_sms_xml(xml_file):
    with open(xml_file, 'r', encoding='utf-8') as file:
        xml_content = file.read()

    sms_messages = xmltodict.parse(xml_content)['smses']['sms']
    
    matched_dates_by_type = {1: [], 2: []}

    for sms in sms_messages:
        if sms['@address'] in numbers_to_match:
            sms_type = int(sms['@type'])
            sms_date = int(sms['@date'])
            if sms_type in matched_dates_by_type:
                matched_dates_by_type[sms_type].append(sms_date)
    
    return matched_dates_by_type

def count_messages_per_day(dates, timezone):
    tz = pytz.timezone(timezone)
    date_times = [datetime.fromtimestamp(date / 1000, tz) - timedelta(hours=day_start_hour)  for date in dates]
    
    df = pd.DataFrame(date_times, columns=['DateTime'])
    df['Date'] = df['DateTime'].dt.date
    
    day_counts = df.groupby('Date').size().reset_index(name='Count')
    
    date_range = pd.date_range(start=day_counts['Date'].min(), end=day_counts['Date'].max())
    day_counts = day_counts.set_index('Date').reindex(date_range, fill_value=0).rename_axis('Date').reset_index()
    
    return day_counts

def save_counts_to_csv(counts, output_csv):
    counts.to_csv(output_csv, index=False)

if __name__ == '__main__':        
    dates_by_type = parse_sms_xml('sms/export.xml')
    save_counts_to_csv(count_messages_per_day(dates_by_type[1], user_timezone), 'sms/output/recieved.csv')
    save_counts_to_csv(count_messages_per_day(dates_by_type[2], user_timezone), 'sms/output/sent.csv')
