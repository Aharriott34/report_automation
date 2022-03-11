import pandas as pd
import numpy as np
import excel_import
from tabulate import tabulate as tab

# Summary by Media Raw Calls
summary_by_media_calls = excel_import.summary_df
# Summary By Media Sales
summary_by_media_sales = excel_import.summary_sales_df
# Salesforce Sales
salesforce_file = excel_import.salesforce_df

# Summary By Media Raw Calls
def summary_by_media(file):
    file_xtab = pd.crosstab(index=file['Publisher'], columns=['Calls'], values=file['Calls'], aggfunc=np.sum, dropna=True).reset_index()
    total_count = np.sum(file_xtab['Calls'])
    print(f'Total Count of Calls: {total_count}\n')
    return file_xtab

def sum_by_media(file, substr):
    file_xtab = pd.crosstab(index=file['Publisher'], columns=['Calls'], values=file['Calls'], aggfunc=np.sum, dropna=True).reset_index()
    idx_of_publisher = file_xtab['Publisher'].str.contains(substr)
    publisher_count = file_xtab[idx_of_publisher]['Calls'].sum()
    return publisher_count

def sum_raw_calls():
    print(f'\nSummary By Media: Raw Calls')
    print_substr = 'CONVERGE'
    print(f'Print: {sum_by_media(summary_by_media_calls,print_substr)}')

    print_insert_substr = "Incremental -"
    print(f'Print - Inserts: {sum_by_media(summary_by_media_calls,print_insert_substr)}')

    mailer_substr = "Incremental Mailer"
    mailer_substr_2 = 'Incremental BLEND'
    mailer_substr_3 = 'Incremental_Mailer'
    mailer_total_1 = sum_by_media(summary_by_media_calls,mailer_substr)
    mailer_total_2 = sum_by_media(summary_by_media_calls,mailer_substr_2)
    mailer_total_3 = sum_by_media(summary_by_media_calls,mailer_substr_3)
    total_mailer = mailer_total_1 + mailer_total_2 + mailer_total_3
    print(f'Mailer: {total_mailer}')

    mailer_bfyt_mch_substr = 'BFYT Doty Mailer MCH'
    print(f'Mailer BFYT MCH: {sum_by_media(summary_by_media_calls,mailer_bfyt_mch_substr)}')

    doty_incremental = "BFYT Doty Incremental"
    print(f'DOTY Incremental: {sum_by_media(summary_by_media_calls, doty_incremental)}')

    doty_mspark = "BFYT Doty MSPARK"
    print(f'DOTY Mspark: {sum_by_media(summary_by_media_calls,doty_mspark)}')

    web_substr = 'MEDICARE COVERAGE HELPLINE'
    web_substr_2 = '2021 Medicare Helpline'
    web_count_1 = sum_by_media(summary_by_media_calls,web_substr)
    web_count_2 = sum_by_media(summary_by_media_calls,web_substr_2)
    web_total = web_count_1 + web_count_2
    print(f'Web: {web_total}')

    mch_fb_goog_bing = 'BFYT FB, GOOG, BING'
    print(f'MCH, FB, GOOG, BING: {sum_by_media(summary_by_media_calls, mch_fb_goog_bing)}')

    bringans = 'Bringans'
    print(f'Bringans: {sum_by_media(summary_by_media_calls, bringans)}')
    print('Sunrise Third Party Affiliate: N/A')

    rtn = 'RTN'
    print(f'RTN - Medicare: {sum_by_media(summary_by_media_calls, rtn)}')
    print('My Health Angel: N/A')

    lls = 'LLS'
    print(f'LLS: {sum_by_media(summary_by_media_calls, lls)}')

    bayfront = 'Bayfront'
    print(f'Bayfront: {sum_by_media(summary_by_media_calls, bayfront)}')

    malbis = 'Malbis'
    print(f'Malbis: {sum_by_media(summary_by_media_calls, malbis)}')

    total = sum_by_media(summary_by_media_calls,print_substr) + sum_by_media(summary_by_media_calls,print_insert_substr) + total_mailer + sum_by_media(summary_by_media_calls,mailer_bfyt_mch_substr) + sum_by_media(summary_by_media_calls, doty_incremental) + sum_by_media(summary_by_media_calls,doty_mspark) + web_total + sum_by_media(summary_by_media_calls, mch_fb_goog_bing) + sum_by_media(summary_by_media_calls, bringans) + sum_by_media(summary_by_media_calls, lls) + sum_by_media(summary_by_media_calls, bayfront) + sum_by_media(summary_by_media_calls, malbis) +sum_by_media(summary_by_media_calls, rtn)

    return (f' \nSum: {total}')

# Summary by Media Sales

def sum_sales():
clean_numbers = []
    for items in summary_by_media_sales['Caller ID']:
        items = items.replace('-', '')
        items = items.replace('Restricted', '0')
        items = items.replace('Anonymous', '0')
        items = items.replace('+', '')
        items = items.replace(' ', '')
        clean_numbers.append(items)

    summary_by_media_sales['ANI'] = clean_numbers
    summary_by_media_sales['ANI'] = summary_by_media_sales['ANI'].astype(np.int64)
    summary_by_media_sales['ANI'] = summary_by_media_sales['ANI'].replace(0, 100000000)
    new_df = summary_by_media_sales[['Call Start Time', 'ANI', 'Original Publisher']]
    new_df['Date'] = new_df['Call Start Time']
    new_df['Date'] = pd.to_datetime(pd.to_datetime(new_df['Date']).dt.date)
    new_df.drop('Call Start Time', inplace=True, axis=1)

    sales_numbers = salesforce_file['Phone Stripped'].to_list()
    sales_dates = salesforce_file['Close Date'].to_list()
    sales_numbers.extend(excel_import.go_numbers)
    sales_dates.extend(excel_import.go_dates)

    sales_df = pd.DataFrame(data=np.column_stack([sales_dates, sales_numbers]), columns=['Date', 'ANI'])
    sales_df['Date'] = pd.to_datetime(sales_df['Date'])
    sales_df['ANI'] = sales_df['ANI'].astype(np.int64)
    left_join_df = pd.merge_asof(sales_df.sort_values('Date'), new_df.sort_values('Date'), on='Date', by='ANI')
    return left_join_df

def sum_sales_condensed(file, substr):
    file = file
    idx_of_publisher = file['Original Publisher'].str.contains(substr)
    sales_count = len(file[idx_of_publisher]['Clean Numbers'])
    return sales_count

def sum_sales_final():
    print(f'\nSummary By Media: Sales')

    print_substr = 'CONVERGE'
    print(f'Print: {sum_sales_condensed(sum_sales(), print_substr)}')

    print_insert_substr = "Incremental -"
    print(f'Print - Inserts: {sum_sales_condensed(sum_sales(), print_insert_substr)}')

    mailer_substr = "Incremental Mailer"
    mailer_substr_2 = 'Incremental BLEND'
    mailer_substr_3 = 'Incremental_Mailer'
    mailer_total_1 = sum_sales_condensed(sum_sales(), mailer_substr)
    mailer_total_2 = sum_sales_condensed(sum_sales(), mailer_substr_2)
    mailer_total_3 = sum_sales_condensed(sum_sales(), mailer_substr_3)
    total_mailer = mailer_total_1 + mailer_total_2 + mailer_total_3
    print(f'Mailer: {total_mailer}')


    mailer_bfyt_mch_substr = 'BFYT Doty Mailer MCH'
    print(f'Mailer BFYT MCH: {sum_sales_condensed(sum_sales(), mailer_bfyt_mch_substr)}')

    doty_incremental = "BFYT Doty Incremental"
    print(f'DOTY Incremental: {sum_sales_condensed(sum_sales(), doty_incremental)}')

    doty_mspark = "BFYT Doty MSPARK"
    print(f'DOTY Mspark: {sum_sales_condensed(sum_sales(), doty_mspark)}')

    web_substr = 'MEDICARE COVERAGE HELPLINE'
    web_substr_2 = '2021 Medicare Helpline'
    web_count_1 = sum_sales_condensed(sum_sales(), web_substr)
    web_count_2 = sum_sales_condensed(sum_sales(), web_substr_2)
    web_total = web_count_1 + web_count_2
    print(f'Web: {web_total}')

    mch_fb_goog_bing = 'BFYT FB, GOOG, BING'
    print(f'MCH, FB, GOOG, BING: {sum_sales_condensed(sum_sales(), mch_fb_goog_bing)}')

    bringans = 'Bringans'
    print(f'Bringans: {sum_sales_condensed(sum_sales(), bringans)}')
    print('Sunrise Third Party Affiliate: N/A')

    rtn = 'RTN'
    print(f'RTN - Medicare: {sum_sales_condensed(sum_sales(), rtn)}')
    print('My Health Angel: N/A')

    lls = 'LLS'
    print(f'LLS: {sum_by_media(summary_by_media_calls, lls)}')

    bayfront = 'Bayfront'
    print(f'Bayfront: {sum_by_media(summary_by_media_calls, bayfront)}')

    malbis = 'Malbis'
    print(f'Malbis: {sum_by_media(summary_by_media_calls, malbis)}')
    return ''

# Call Center Sales

def call_center_sales(file,substr):
    file = file
    idx_owner = file['Owner Role'].str.contains(substr)
    sales_count = file[idx_owner]['Close Date'].count()
    return sales_count

def call_center_final():
    print('\nCall Center Sales')

    go_health_total = len(excel_import.go_dates)
    print(f'GoHealth: {go_health_total}')
    print('TIB: N/A')
    
    sunrise_substr_1 = 'Sunrise -'
    sunrise_substr_2 = 'Port St Lucie -'
    sunrise_substr_3 = 'Cypress -'

    sunrise_total_1 = call_center_sales(salesforce_file, sunrise_substr_1)
    sunrise_total_2 = call_center_sales(salesforce_file, sunrise_substr_2)
    sunrise_total_3 = call_center_sales(salesforce_file, sunrise_substr_3)
    total = sunrise_total_1 +sunrise_total_2 + sunrise_total_3
    print(f'Sunrise: {total}')

    smartcare_substr = 'SmartCare'
    smartcare_total = call_center_sales(salesforce_file, smartcare_substr)
    print(f'SmartCare: {smartcare_total}')
    return ''

# Call Center GoHealth Calls Sent, TGH Calls Sent

def calls_sent():
    clean_df = calls_sent_df.loc[(calls_sent_df['Buyer Campaign'] != 'THI - Sales Call Back') & (calls_sent_df['Buyer Campaign'] != 'Contingency Recording Routing(Consent Call Back)  - WEBSITE')]
    clean_df = pd.DataFrame(data=clean_df, columns=['Buyer', 'Buyer Campaign', 'Calls'])
    go_sum = clean_df.loc[clean_df['Buyer'] == 'GO Agents', 'Calls'].sum()

    mha = excel_import.mha_df
    mha = mha.dropna(subset=['Duration'])
    new_mha = mha[['Call Date', 'Target Number']].dropna().reset_index()
    new_mha['Target Number'] = new_mha['Target Number'].astype(np.int64)
    grouped = new_mha.groupby('Target Number', dropna=True, as_index=False).size()
    grouped.columns.values[1] = 'Count'

    go_mha = grouped[grouped['Target Number'] == 18556832760]['Count'].values.tolist()
    tgh_mha = grouped[grouped['Target Number'] == 18134732827]['Count'].values.tolist()
    tgh_mha_2 = grouped[grouped['Target Number'] == 14844651583]['Count'].values.tolist()
    tgh_mha_3 = grouped[grouped['Target Number'] == 13083209231]['Count'].values.tolist()
    tgh_mha_4 = grouped[grouped['Target Number'] == 13086244672]['Count'].values.tolist()
    tgh_mha_5 = grouped[grouped['Target Number'] == 16106726686]['Count'].values.tolist()
    tgh_mha_6 = grouped[grouped['Target Number'] == 18775160935]['Count'].values.tolist()

    def mha_if(file):
        if not file:
            file.append(0)
    mha_if(go_mha)
    mha_if(tgh_mha)
    mha_if(tgh_mha_2)
    mha_if(tgh_mha_3)
    mha_if(tgh_mha_4)
    mha_if(tgh_mha_5)
    mha_if(tgh_mha_6)

    tgh_list = [tgh_mha, tgh_mha_2, tgh_mha_3, tgh_mha_4, tgh_mha_5, tgh_mha_6]
    flatten_tgh = [val for sublist in tgh_list for val in sublist]

    go_input = int(input('GO Subtraction: '))
    go_final = go_sum - go_input + go_mha[0]
    tgh_sum = clean_df.loc[clean_df['Buyer'].isin(['TGH Agency', 'MCH']), 'Calls'].sum()
    tgh_input = int(input('TGH Subtraction: '))
    tgh_final = tgh_sum - tgh_input + sum(flatten_tgh)
    print(f'\nGoHealth MHA Count: {go_mha[0]}\nGoHealth Calls Sent: {go_final}')
    print(f'\nTGH MHA Count: {sum(flatten_tgh)}\nTGH Calls Sent: {tgh_final}')
    return clean_df

def five9_fronter_table():
    timestamp = fronter_df['TIMESTAMP']
    fronter_date = pd.to_datetime(timestamp)
    fronter_time = pd.to_timedelta(pd.to_datetime(timestamp).dt.strftime('%H:%M:%S'))
    fronter_df['Date'] = pd.to_datetime(fronter_date)
    fronter_df['Time'] = fronter_time
    new_fronter_df = fronter_df[['Date', 'Time','CAMPAIGN', 'CALL TYPE', 'ANI']]
    print(f'Fronter ANI Count: {len(new_fronter_df["ANI"])}')

    call_time = invoca_df.iloc[:, 0]
    invoca_date = pd.to_datetime(call_time)
    invoca_time = pd.to_timedelta(pd.to_datetime(call_time).dt.strftime('%H:%M:%S'))

    clean_ani = []
    for items in invoca_df['Caller ID']:
        items = str(items)
        items = items.replace('-', '')
        items = items.replace('Restricted', '0')
        items = items.replace('Anonymous', '0')
        items = items.replace('anonymous', '0')
        items = items.replace('+', '')
        items = items.replace(' ', '')
        items = items.replace('fsazb56c7ce', '0')
        items = items.replace('nan', '0')
        items = items.replace('asterisk', '0')
        items = items.replace('Unavailable', '0')
        items = int(items)
        clean_ani.append(items)

    invoca_df['Date'] = pd.to_datetime(invoca_date)
    invoca_df['Time'] = invoca_time
    invoca_df['ANI'] = clean_ani
    new_invoca_df = invoca_df[['Date', 'Time', 'Original Publisher', 'ANI']]

    left_join_df = pd.merge_asof(new_fronter_df.sort_values('Date'), new_invoca_df.sort_values('Date'), on='Date', by='ANI',direction='backward')
    left_join_df['Time Count'] = left_join_df['Time_x'] >= left_join_df['Time_y']
    left_join_df['Time Count'] = left_join_df['Time Count'] * 1

    df_xtab = pd.crosstab(index=left_join_df['Original Publisher'], columns=pd.to_datetime(left_join_df['Date']).dt.date, values=left_join_df['Time Count'], aggfunc=sum)
    df_xtab.loc['Total'] = df_xtab.sum()
    grand_total_days = df_xtab.sum(axis=1)
    print(df_xtab)
    print(f'\n{grand_total_days}')
    return (tab(left_join_df, headers='keys', tablefmt='fancy_grid'))
