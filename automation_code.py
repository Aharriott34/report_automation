import pandas as pd
import numpy as np
import excel_import

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
        clean_numbers.append(items)

    summary_by_media_sales['Clean Numbers'] = clean_numbers
    summary_by_media_sales['Clean Numbers'] = summary_by_media_sales['Clean Numbers'].astype(np.int64)
    new_df = summary_by_media_sales[['Clean Numbers', 'Call Start Time', 'Original Publisher']]

    sales_numbers = salesforce_file['Phone Stripped'].to_list()
    sales_dates = salesforce_file['Close Date'].to_list()
    sales_numbers.extend(excel_import.go_numbers)
    sales_dates.extend(excel_import.go_dates)

    sales_df = pd.DataFrame(data=sales_numbers, columns=['Clean Numbers'])
    left_join_df = sales_df.merge(new_df, on='Clean Numbers', how='left', copy=False)
    left_join_df = left_join_df[['Clean Numbers', 'Original Publisher']].dropna()
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