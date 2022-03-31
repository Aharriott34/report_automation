import datetime
import pandas as pd

pd.options.mode.chained_assignment = None
pd.set_option('max_columns', None)
pd.set_option('max_rows', None)

prelog_df = pd.read_excel(r'\\BFYT-DC01\Redirected_Folders\aharriott\Documents\Excel Files\python_prelog.xlsx')
invoca_df = pd.read_csv(r'C:\Users\aharriott\Downloads\production_results_2183525_Report_2022-03-16_call_details_20220317-1-1rq1kud_original.csv')
prelog_df.drop(prelog_df.index[prelog_df['Date Aired'] == 'GRAND TOTAL'], inplace=True)
def prelog_limit(spend_limit):
    clean_ani = []
    for items in prelog_df['Phone-Aired']:
        items = str(items)
        items = items.replace('(', '')
        items = items.replace(')', '')
        items = items.replace(' ', '')
        items = items.replace('-', '')
        items = items.replace('nan', '0')
        items = int(items)
        clean_ani.append(items)
    prelog_df['Source'] = clean_ani
    prelog_df['Date Aired'] = pd.to_datetime(prelog_df['Date Aired']).dt.date
    prelog_df['Date'] = prelog_df['Date Aired'].astype(str) + ' ' + prelog_df['Time Aired'].astype(str)
    prelog_selection = prelog_df[(prelog_df['Spend'] >= spend_limit) & (prelog_df['Spend'] < 100000)]
    prelog_selection['Date'] = pd.to_datetime(prelog_selection['Date'])
    prelog_selection['Time'] = pd.to_timedelta(prelog_selection['Date'].dt.strftime('%H:%M:%S'))
    new_df = prelog_selection[['Date', 'Time', 'Station', 'Estimate', 'Source', 'Spend']]
    return new_df.sort_values('Date')

def clean_invoca():
    call_time = invoca_df.iloc[:, 0]
    invoca_date = pd.to_datetime(call_time)
    invoca_time = pd.to_timedelta(pd.to_datetime(call_time).dt.strftime('%H:%M:%S'))
    invoca_df['Date'] = pd.to_datetime(invoca_date)
    invoca_df['Time'] = invoca_time

    clean_source = []
    for items in invoca_df['Source']:
        items = items.replace('-','')
        items = int(items)
        clean_source.append(items)

    invoca_df['Source'] = clean_source
    new_df = invoca_df[['Date', 'Time', 'Source']]
    return new_df.sort_values('Date')

merged = pd.merge_ordered(left=prelog_limit(500), right=clean_invoca(), how='left', on='Source')
merge_df = merged[merged['Time_y'] >= merged['Time_x']]

df_list = []
for rows in merge_df.iterrows():
    curr_source = rows[1]['Source']
    curr_time_x = rows[1]['Time_x']
    spent = rows[1]['Spend']
    all_time_y = merge_df[merge_df['Source'] == curr_source]['Time_y']

    count = 0
    for y in all_time_y:
        if (y - curr_time_x) <= datetime.timedelta(0,1200) and \
                (y - curr_time_x) > datetime.timedelta(0,0):
            count += 1
    df_list.append({'Source': curr_source, 'Time': curr_time_x, 'Spend': spent, 'Count': count})


new_df = pd.DataFrame(df_list)
new_df.set_index(['Source', 'Time'], inplace=True)
new_df.sort_index(inplace=True)
dup_drop_df = new_df.drop_duplicates()
low_count_df = dup_drop_df[dup_drop_df['Count'] <= 3]
print(low_count_df)