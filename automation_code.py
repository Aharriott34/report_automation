import pandas as pd
import numpy as np
import excel_import
import datetime
import re
# Summary by Media Raw Calls
summary_by_media_calls = excel_import.summary_df

# Summary By Media Sales
summary_by_media_sales = excel_import.summary_sales_df
tgh_tv_sales = excel_import.tgh_tv_details_df

# Salesforce Sales
salesforce_file = excel_import.salesforce_df

# Calls Sent
calls_sent_df = excel_import.calls_sent_df

# Five9 Fronter & Invoca
fronter_df = excel_import.fronter_df
invoca_df = excel_import.invoca_df

pd.set_option('max_columns', None)

# Time Cleaner
def convert(time):
    return datetime.datetime.fromtimestamp(time)

# Summary By Media Raw Calls
def summary_by_media(file):
    file_xtab = pd.crosstab(index=file['Publisher'], columns=['Calls'], values=file['Calls'], aggfunc=np.sum, dropna=True).reset_index()
    total_count = np.sum(file_xtab['Calls'])
    print(f'Total Count of Calls: {total_count}\n')
    print(file_xtab)

def sum_by_media(file, substr):
    file_xtab = pd.crosstab(index=file['Publisher'], columns=['Calls'], values=file['Calls'], aggfunc=np.sum, dropna=True).reset_index()
    idx_of_publisher = file_xtab['Publisher'].str.contains(substr)
    publisher_count = file_xtab[idx_of_publisher]['Calls'].sum()
    return publisher_count

def sum_raw_calls():
    print(f'\nSummary By Media: Raw Calls')
    print_substr = 'CONVERGE'
    print(f'Converge Print: {sum_by_media(summary_by_media_calls,print_substr)}')

    print_insert_substr = "Incremental -"
    print(f'Incremental Inserts: {sum_by_media(summary_by_media_calls,print_insert_substr)}')

    mailer_substr = "Incremental Mailer"
    mailer_substr_2 = 'Incremental BLEND'
    mailer_substr_3 = 'Incremental_Mailer'
    mailer_total_1 = sum_by_media(summary_by_media_calls,mailer_substr)
    mailer_total_2 = sum_by_media(summary_by_media_calls,mailer_substr_2)
    mailer_total_3 = sum_by_media(summary_by_media_calls,mailer_substr_3)
    total_mailer = mailer_total_1 + mailer_total_2 + mailer_total_3
    print(f'Incremental Mailer: {total_mailer}')

    mailer_bfyt_mch_substr = 'BFYT Doty Mailer MCH'
    print(f'DOTY Mailer: {sum_by_media(summary_by_media_calls,mailer_bfyt_mch_substr)}')

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

    print(f' \nSum: {total}')

# Summary by Media Sales

def sum_sales():
    clean_numbers = []
    for items in summary_by_media_sales['Caller ID']:
        items = str(items)
        items = items.replace('-', '')
        items = items.replace('Restricted', '0')
        items = items.replace('Anonymous', '0')
        items = items.replace('anonymous', '0')
        items = items.replace('+', '')
        items = items.replace(' ', '')
        items = items.replace('!', '0')
        items = items.replace('nan', '0')
        items = items.replace('91(0)56248445', '0')
        items = int(items)
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
    sales_df['Date'] = sales_df['Date'].apply(lambda x: convert(x) if isinstance(x, int) else x)
    sales_df['Date'] = pd.to_datetime(sales_df['Date'])
    sales_df['ANI'] = sales_df['ANI'].astype(np.int64)
    left_join_df = pd.merge_asof(sales_df.sort_values('Date'), new_df.sort_values('Date'), on='Date', by='ANI')
    return left_join_df

# New TV entry for Sum by Media
def sum_sales_tv():
    clean_numbers = []
    for items in tgh_tv_sales['Caller ID']:
        items = str(items)
        items = items.replace('-', '')
        items = items.replace('Restricted', '0')
        items = items.replace('Anonymous', '0')
        items = items.replace('anonymous', '0')
        items = items.replace('+', '')
        items = items.replace(' ', '')
        items = items.replace('!', '0')
        items = items.replace('nan', '0')
        items = items.replace('91(0)56248445', '0')
        items = int(items)
        clean_numbers.append(items)

    tgh_tv_sales['ANI'] = clean_numbers
    tgh_tv_sales['ANI'] = tgh_tv_sales['ANI'].astype(np.int64)
    tgh_tv_sales['ANI'] = tgh_tv_sales['ANI'].replace(0, 100000000)
    new_df = tgh_tv_sales[['Call Start Time', 'ANI', 'Original Publisher']]
    new_df['Date'] = new_df['Call Start Time']
    new_df['Date'] = pd.to_datetime(pd.to_datetime(new_df['Date']).dt.date)
    new_df.drop('Call Start Time', inplace=True, axis=1)

    sales_numbers = salesforce_file['Phone Stripped'].to_list()
    sales_dates = salesforce_file['Close Date'].to_list()
    sales_numbers.extend(excel_import.go_numbers)
    sales_dates.extend(excel_import.go_dates)

    sales_df = pd.DataFrame(data=np.column_stack([sales_dates, sales_numbers]), columns=['Date', 'ANI'])
    sales_df['Date'] = sales_df['Date'].apply(lambda x: convert(x) if isinstance(x, int) else x)
    sales_df['Date'] = pd.to_datetime(sales_df['Date'])
    sales_df['ANI'] = sales_df['ANI'].astype(np.int64)
    left_join_df = pd.merge_asof(sales_df.sort_values('Date'), new_df.sort_values('Date'), on='Date', by='ANI')
    tv_condensed = left_join_df[(left_join_df['Original Publisher'] == 'Jimmie Walker 2021') | (left_join_df['Original Publisher'] == 'William Devane 2022 PI') | (left_join_df['Original Publisher'] == 'William Devane 2022') | (left_join_df['Original Publisher'] == 'Joe Namath Main') | (left_join_df['Original Publisher'] == 'Joe Namath PI') | (left_join_df['Original Publisher'] == 'William Shatner 2021') | (left_join_df['Original Publisher'] == 'Greta Sesheta 2021') | (left_join_df['Original Publisher'] == 'Jimmie Walker 2021 PI') | (left_join_df['Original Publisher'] == 'Reingold TV - More')]
    return tv_condensed.count(axis=0)



def sum_sales_condensed(file, substr):
    file = file
    idx_of_publisher = file['Original Publisher'].str.contains(substr, na=False)
    sales_count = file[idx_of_publisher]['ANI'].count()
    return sales_count

def sum_sales_final():
    print(f'\nSummary By Media: Sales')

    print(f' TV: {sum_sales_tv().ANI}')

    print_substr = 'CONVERGE'
    print(f'Converge Print: {sum_sales_condensed(sum_sales(), print_substr)}')

    print_insert_substr = "Incremental -"
    print(f'Incremental Inserts: {sum_sales_condensed(sum_sales(), print_insert_substr)}')

    mailer_substr = "Incremental Mailer"
    mailer_substr_2 = 'Incremental BLEND'
    mailer_substr_3 = 'Incremental_Mailer'
    mailer_total_1 = sum_sales_condensed(sum_sales(), mailer_substr)
    mailer_total_2 = sum_sales_condensed(sum_sales(), mailer_substr_2)
    mailer_total_3 = sum_sales_condensed(sum_sales(), mailer_substr_3)
    total_mailer = mailer_total_1 + mailer_total_2 + mailer_total_3
    print(f'Incremental Mailer: {total_mailer}')


    mailer_bfyt_mch_substr = 'BFYT Doty Mailer MCH'
    print(f'Mailer BFYT MCH: {sum_sales_condensed(sum_sales(), mailer_bfyt_mch_substr)}')

    doty_incremental = "BFYT Doty Incremental"
    print(f'DOTY Incremental: {sum_sales_condensed(sum_sales(), doty_incremental)}')

    doty_mspark = "BFYT Doty MSPARK"
    print(f'DOTY Mspark: {sum_sales_condensed(sum_sales(), doty_mspark)}')

    web_substr = 'MEDICARE COVERAGE HELPLINE'
    web_substr_2 = '2021 Medicare Helpline'
    web_substr_3 = 'MCH.COM'
    web_count_1 = sum_sales_condensed(sum_sales(), web_substr)
    web_count_2 = sum_sales_condensed(sum_sales(), web_substr_2)
    web_count_3 = sum_sales_condensed(sum_sales(), web_substr_3)
    web_total = web_count_1 + web_count_2 + web_count_3
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
    print(f'LLS: {sum_sales_condensed(sum_sales(), lls)}')

    bayfront = 'Bayfront'
    print(f'Bayfront: {sum_sales_condensed(sum_sales(), bayfront)}')

    malbis = 'Malbis'
    print(f'Malbis: {sum_sales_condensed(sum_sales(), malbis)}')
    return ''

# Call Center Sales

def call_center_sales(file,substr):
    file = file
    idx_owner = file['Owner Role'].str.contains(substr, na=False)
    sales_count = file[idx_owner]['Close Date'].count()
    return sales_count

# New Format
def call_center_final():
    print('\nCall Center Sales')
    condensed_sales = salesforce_file[['Close Date','Site']]
    pivot_info = condensed_sales.groupby(by=['Site']).count()
    print(pivot_info)
    return '\n'

# Old Format
# def call_center_final():
#     print('\nCall Center Sales')
#
#     go_health_total = len(excel_import.go_dates)
#     print(f'GoHealth: {go_health_total}')
#     print('TIB: N/A')
#
#     sunrise_substr_1 = 'Sunrise -'
#     sunrise_substr_2 = 'Port St Lucie -'
#     sunrise_substr_3 = 'Cypress -'
#
#     sunrise_total_1 = call_center_sales(salesforce_file, sunrise_substr_1)
#     sunrise_total_2 = call_center_sales(salesforce_file, sunrise_substr_2)
#     sunrise_total_3 = call_center_sales(salesforce_file, sunrise_substr_3)
#     total = sunrise_total_1 +sunrise_total_2 + sunrise_total_3
#     print(f'Sunrise: {total}')
#
#     smartcare_substr = 'SmartCare'
#     smartcare_total = call_center_sales(salesforce_file, smartcare_substr)
#     print(f'SmartCare: {smartcare_total}')
#     return '\n'

# Call Center GoHealth Calls Sent, TGH Calls Sent
def invoca_cleaner(invoca_csv):
    clean_source = []
    source = invoca_csv['Source']
    for items in source:
        items = str(items)
        items = items.replace('-', '')
        items = int(items)
        clean_source.append(items)
    source_df = pd.DataFrame(data=clean_source, columns=['Source'])
    grouped = source_df.groupby('Source', dropna=True, as_index=False).size()
    grouped.columns.values[1] = 'Count'
    return grouped

def calls_sent():
    try:
        clean_df = calls_sent_df.loc[(calls_sent_df['Buyer Campaign'] != 'THI - Sales Call Back') & (calls_sent_df['Buyer Campaign'] != 'Contingency Recording Routing(Consent Call Back)  - WEBSITE') & (calls_sent_df['Buyer Campaign'] != 'Callback_Media.net') & (calls_sent_df['Buyer Campaign'] != 'Callback_Facebook') & (calls_sent_df['Buyer Campaign'] != 'Callback_Bing')]
        clean_df = pd.DataFrame(data=clean_df, columns=['Buyer', 'Buyer Campaign', 'Calls'])


        mha = excel_import.mha_df
        # grouped = invoca_cleaner(mha)
        mha = mha.dropna(subset=['Duration'])
        new_mha = mha[['Call Date', 'Target Number']].dropna().reset_index()
        new_mha['Target Number'] = new_mha['Target Number'].astype(np.int64)
        grouped = new_mha.groupby('Target Number', dropna=True, as_index=False).size()
        grouped.columns.values[1] = 'Count'

        # go_mha = grouped[grouped['Source'] == 8556832760]['Count'].values.tolist()
        # go_mha_2 = grouped[grouped['Source'] == 8557767861]['Count'].values.tolist()
        # tgh_mha = grouped[grouped['Source'] == 8134732827]['Count'].values.tolist()
        # tgh_mha_2 = grouped[grouped['Source'] == 4844651583]['Count'].values.tolist()
        # tgh_mha_3 = grouped[grouped['Source'] == 3083209231]['Count'].values.tolist()
        # tgh_mha_4 = grouped[grouped['Source'] == 3086244672]['Count'].values.tolist()
        # tgh_mha_5 = grouped[grouped['Source'] == 6106726686]['Count'].values.tolist()
        # tgh_mha_6 = grouped[grouped['Source'] == 8775160935]['Count'].values.tolist()
        # tgh_mha_7 = grouped[grouped['Source'] == 8137738402]['Count'].values.tolist()

        # go_mha = grouped[grouped['Target Number'] == 18556832760]['Count'].values.tolist()
        # go_mha_2 = grouped[grouped['Target Number'] == 18557767861]['Count'].values.tolist()
        tgh_mha = grouped[grouped['Target Number'] == 18134732827]['Count'].values.tolist()
        tgh_mha_2 = grouped[grouped['Target Number'] == 14844651583]['Count'].values.tolist()
        tgh_mha_3 = grouped[grouped['Target Number'] == 13083209231]['Count'].values.tolist()
        tgh_mha_4 = grouped[grouped['Target Number'] == 13086244672]['Count'].values.tolist()
        tgh_mha_5 = grouped[grouped['Target Number'] == 16106726686]['Count'].values.tolist()
        tgh_mha_6 = grouped[grouped['Target Number'] == 18775160935]['Count'].values.tolist()
        tgh_mha_7 = grouped[grouped['Target Number'] == 18137738402]['Count'].values.tolist()
        tgh_mha_8 = grouped[grouped['Target Number'] == 18557888864]['Count'].values.tolist()
        tgh_mha_9 = grouped[grouped['Target Number'] == 18449742193]['Count'].values.tolist()


        def mha_if(file):
            if not file:
                file.append(0)
        # mha_if(go_mha)
        # mha_if(go_mha_2)
        mha_if(tgh_mha)
        mha_if(tgh_mha_2)
        mha_if(tgh_mha_3)
        mha_if(tgh_mha_4)
        mha_if(tgh_mha_5)
        mha_if(tgh_mha_6)
        mha_if(tgh_mha_7)
        mha_if(tgh_mha_8)
        mha_if(tgh_mha_9)


        tgh_list = [tgh_mha, tgh_mha_2, tgh_mha_3, tgh_mha_4, tgh_mha_5, tgh_mha_6, tgh_mha_7,tgh_mha_8, tgh_mha_9]
        # go_list = [go_mha, go_mha_2]
        flatten_tgh = [val for sublist in tgh_list for val in sublist]
        # flatten_go = [val for sublist in go_list for val in sublist]

        # go_input = int(input('\nGO Subtraction: '))
        # go_sum = clean_df.loc[clean_df['Buyer'] == 'GO Agents', 'Calls'].sum()
        # go_final = go_sum - go_input + sum(flatten_go)
        tgh_sum = clean_df.loc[clean_df['Buyer'].isin(['TGH Agency', 'MCH']), 'Calls'].sum()
        tgh_input = int(input('TGH Subtraction: '))
        tgh_final = tgh_sum - tgh_input + sum(flatten_tgh)
        # print(f'\nGoHealth MHA Count: {sum(flatten_go)}\nGoHealth Calls Sent: {go_final}')
        print(f'\nTGH MHA Count: {sum(flatten_tgh)}\nTGH Calls Sent: {tgh_final}')
    except AttributeError:
       # no_mha_go_inp = int(input('\nGO Subtraction: '))
       no_mha_tgh_inp = int(input('TGH Subtraction: '))
       # no_mha_go = clean_df.loc[clean_df['Buyer'] == 'GO Agents', 'Calls'].sum() - no_mha_go_inp
       no_mha_tgh = clean_df.loc[clean_df['Buyer'].isin(['TGH Agency', 'MCH']), 'Calls'].sum() - no_mha_tgh_inp
       print('NO MHA')
       # print(f'\nGoHealth Calls Sent: {no_mha_go} \nTGH Calls Sent: {no_mha_tgh}')
    return clean_df

# Five9 & Invoca Fronter Call Confirmation

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
        items = items.replace('Unknown', '0')
        items = items.replace('NONE', '0')
        items = items.replace('+', '')
        items = items.replace(' ', '')
        items = items.replace('fsazb56c7ce', '0')
        items = items.replace('nan', '0')
        items = items.replace('asterisk', '0')
        items = items.replace('Unavailable', '0')
        items = items.replace('4.05552E11', '0')
        items = items.replace('1.41061E11', '0')
        items = items.replace('44(0)7842175735', '0')
        items = items.replace('49(0)23', '0')
        items = items.replace('44(0)2039477000', '0')
        items = items.replace('44(0)7785717828', '0')
        items = items.replace('ncsvoice', '0')
        items = items.replace('gsresidents', '0')
        items = items.replace('44(0)1414847774', '0')
        items = items.replace('!', '0')
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
    df_xtab['Grand Total'] = df_xtab.sum(axis=1)


    writer = pd.ExcelWriter(
        r'\\BFYT-DC01\Redirected_Folders\aharriott\Documents\Excel Files\State Monthly Report\2022 Report\Fronter_Invoca_Output.xlsx')
    left_join_df.to_excel(writer, sheet_name='Left Join Check',
                          header=['Date', 'Fronter', 'Fronter Time', 'Call Type', 'ANI', 'Invoca Time',
                                  'Original Publisher', 'Time Counted'], index=False)
    df_xtab.to_excel(writer, sheet_name='Pivoted Information')
    writer.save()
