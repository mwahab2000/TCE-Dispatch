import requests 
import json
import pandas as pd
from datetime import  datetime , timedelta
import warnings
warnings.filterwarnings('ignore')
import numpy as np

#########################################################################################
#     "type" : "S"  For Shipments
#     "type" : "G"  GPS data points
#     "type" : "L"  For Saved Locations


##########################################################################################################################
def generate_dispatch_status(history):
    url = "https://prod-112.westeurope.logic.azure.com:443/workflows/a2fbd6a584ff4045bc1261524055cbac/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=8US3nNdIFHhpa8YCcI0G1RnIk_gojWhzzKh5mUQOMMU"
    headers = {'Content-Type': 'application/json'}
    cols = ['tedlr_shipment_no','tedlr_plant','tedlr_division','tedlr_distribution_channel','tedlr_customer_no','tedlr_customer_name','tedlr_material_no','tedlr_material_description',
            'tedlr_truck_number','tedlr_driver_code','tedlr_driver_name','tedlr_packerid','tedlr_dispatcher',
            'tedlr_planned_qty','tedlr_weighin','tedlr_weighout','tedlr_net_weight','tedlr_planning_date','tedlr_planning_time','tedlr_weighin_date',
            'tedlr_weighin_time','tedlr_weighout_date','tedlr_weighout_time','createdon','tedlr_name','modifiedon']
    df = pd.DataFrame()


    selected_day = (datetime.today()- timedelta(days=history)).strftime('%Y%m%d')
    payload = json.dumps({"date": selected_day   ,  "type" : "S"})
    response = requests.request("POST", url, headers=headers, data=payload)
    day_df = pd.DataFrame(json.loads(response.text))
    df = day_df[cols].copy()
    ######################################################################
    df['planning_datetime'] = pd.to_datetime(df['tedlr_planning_date'] + df['tedlr_planning_time'], format='%Y%m%d%H%M%S')
    df['weighin_datetime'] = pd.to_datetime(df['tedlr_weighin_date'] + df['tedlr_weighin_time'], format='%Y%m%d%H%M%S')
    df['weighout_datetime'] = pd.to_datetime(df['tedlr_weighout_date'] + df['tedlr_weighout_time'], format='%Y%m%d%H%M%S')
    df['planning_datetime'] = pd.to_datetime(df['planning_datetime'])
    df['weighin_datetime'] = pd.to_datetime(df['weighin_datetime'])
    df['weighout_datetime'] = pd.to_datetime(df['weighout_datetime'])
    df = df.sort_values('planning_datetime')
    #########################################################################
    # waiting shipments are shipments with no datetime in weigh in 
    waiting_shipments = df[pd.isna(df['weighin_datetime'])]
    # select today shipments with weigh in datetime filled
    weigh_in_shipments = df[(df['weighin_datetime'].dt.day == datetime.today().day) & pd.notna(df['weighin_datetime'])]
    # select today shipments with weigh out datetime filled
    weigh_out_shipments = df[(df['weighout_datetime'].dt.day == datetime.today().day) & pd.notna(df['weighout_datetime'])]
    #########################################################################
    # consolidated figures, count the number of shipments grouped by material description
    waiting_shipments_count = pd.DataFrame(waiting_shipments.groupby('tedlr_material_description').size().reset_index(name='Trucks Awaiting Weigh In'))
    waiting_shipments_sum = waiting_shipments.groupby('tedlr_material_description')['tedlr_planned_qty'].sum().reset_index(name='Qty')
    weighing_in_shipments_count = pd.DataFrame(weigh_in_shipments.groupby('tedlr_material_description').size().reset_index(name='Trucks Inside'))
    weigh_out_shipments_count = pd.DataFrame(weigh_out_shipments.groupby('tedlr_material_description').size().reset_index(name='Trucks Departed'))
    # put all in one dataframe
    Consolidated = pd.merge(waiting_shipments_sum , waiting_shipments_count , on='tedlr_material_description', how='outer')
    Consolidated = pd.merge(Consolidated, weighing_in_shipments_count, on='tedlr_material_description', how='outer')
    Consolidated = pd.merge(Consolidated, weigh_out_shipments_count, on='tedlr_material_description', how='outer')
    Consolidated.rename(columns={'tedlr_material_description': 'Material'}, inplace=True)

    sums = Consolidated.select_dtypes(include=[np.number]).sum()
    sums_df = pd.DataFrame([sums], columns=Consolidated.columns[1:])  
    sums_df['Material'] = 'Total'  
    sums_df = sums_df[Consolidated.columns]  
    Consolidated = pd.concat([Consolidated, sums_df], ignore_index=True)


    Consolidated['Trucks Inside'] = pd.to_numeric(Consolidated['Trucks Inside'], errors='coerce')
    Consolidated['Trucks Awaiting Weigh In'] = pd.to_numeric(Consolidated['Trucks Awaiting Weigh In'], errors='coerce')
    Consolidated['Trucks Departed'] = pd.to_numeric(Consolidated['Trucks Departed'], errors='coerce')
    Consolidated.fillna(0, inplace=True)
    Consolidated['Trucks Inside'] = Consolidated['Trucks Inside'].astype('Int64').astype(object)
    Consolidated['Trucks Awaiting Weigh In'] = Consolidated['Trucks Awaiting Weigh In'].astype('Int64').astype(object)
    Consolidated['Trucks Departed'] = Consolidated['Trucks Departed'].astype('Int64').astype(object)
    Consolidated = Consolidated.replace(0, '')





    #APCC figures
    waiting_shipments_count_apcc = pd.DataFrame(waiting_shipments[waiting_shipments['tedlr_plant'] == '2000'].groupby('tedlr_material_description').size().reset_index(name='Trucks Awaiting Weigh In'))
    waiting_shipments_sum_apcc = waiting_shipments[waiting_shipments['tedlr_plant'] == '2000'].groupby('tedlr_material_description')['tedlr_planned_qty'].sum().reset_index(name='Qty')
    weighing_in_shipments_count_apcc = pd.DataFrame(weigh_in_shipments[weigh_in_shipments['tedlr_plant'] == '2000'].groupby('tedlr_material_description').size().reset_index(name='Trucks Inside'))
    weigh_out_shipments_count_apcc = pd.DataFrame(weigh_out_shipments[weigh_out_shipments['tedlr_plant'] == '2000'].groupby('tedlr_material_description').size().reset_index(name='Trucks Departed'))

    APCC = pd.merge(waiting_shipments_sum_apcc , waiting_shipments_count_apcc , on='tedlr_material_description', how='outer')
    APCC = pd.merge(APCC, weighing_in_shipments_count_apcc, on='tedlr_material_description', how='outer')
    APCC = pd.merge(APCC, weigh_out_shipments_count_apcc, on='tedlr_material_description', how='outer')
    APCC.rename(columns={'tedlr_material_description': 'Material'}, inplace=True)

    sums = APCC.select_dtypes(include=[np.number]).sum()
    sums_df = pd.DataFrame([sums], columns=APCC.columns[1:])  
    sums_df['Material'] = 'Total'  
    sums_df = sums_df[APCC.columns]  
    APCC = pd.concat([APCC, sums_df], ignore_index=True)


    APCC['Trucks Inside'] = pd.to_numeric(APCC['Trucks Inside'], errors='coerce')
    APCC['Trucks Awaiting Weigh In'] = pd.to_numeric(APCC['Trucks Awaiting Weigh In'], errors='coerce')
    APCC['Trucks Departed'] = pd.to_numeric(APCC['Trucks Departed'], errors='coerce')
    APCC.fillna(0, inplace=True)
    APCC['Trucks Inside'] = APCC['Trucks Inside'].astype('Int64').astype(object)
    APCC['Trucks Awaiting Weigh In'] = APCC['Trucks Awaiting Weigh In'].astype('Int64').astype(object)
    APCC['Trucks Departed'] = APCC['Trucks Departed'].astype('Int64').astype(object)
    APCC = APCC.replace(0, '')


    #BSCC figures
    waiting_shipments_count_bscc = pd.DataFrame(waiting_shipments[waiting_shipments['tedlr_plant'] == '1000'].groupby('tedlr_material_description').size().reset_index(name='Trucks Awaiting Weigh In'))
    waiting_shipments_sum_bscc = waiting_shipments[waiting_shipments['tedlr_plant'] == '1000'].groupby('tedlr_material_description')['tedlr_planned_qty'].sum().reset_index(name='Qty')
    weighing_in_shipments_count_bscc = pd.DataFrame(weigh_in_shipments[weigh_in_shipments['tedlr_plant'] == '1000'].groupby('tedlr_material_description').size().reset_index(name='Trucks Inside'))
    weigh_out_shipments_count_bscc = pd.DataFrame(weigh_out_shipments[weigh_out_shipments['tedlr_plant'] == '1000'].groupby('tedlr_material_description').size().reset_index(name='Trucks Departed'))

    BSCC = pd.merge(waiting_shipments_sum_bscc , waiting_shipments_count_bscc , on='tedlr_material_description', how='outer')
    BSCC = pd.merge(BSCC, weighing_in_shipments_count_bscc, on='tedlr_material_description', how='outer')
    BSCC = pd.merge(BSCC, weigh_out_shipments_count_bscc, on='tedlr_material_description', how='outer')
    BSCC.rename(columns={'tedlr_material_description': 'Material'}, inplace=True)

    sums = BSCC.select_dtypes(include=[np.number]).sum()
    sums_df = pd.DataFrame([sums], columns=BSCC.columns[1:])  
    sums_df['Material'] = 'Total'  
    sums_df = sums_df[BSCC.columns]  
    BSCC = pd.concat([BSCC, sums_df], ignore_index=True)



    BSCC['Trucks Inside'] = pd.to_numeric(BSCC['Trucks Inside'], errors='coerce')
    BSCC['Trucks Awaiting Weigh In'] = pd.to_numeric(BSCC['Trucks Awaiting Weigh In'], errors='coerce')
    BSCC['Trucks Departed'] = pd.to_numeric(BSCC['Trucks Departed'], errors='coerce')
    BSCC.fillna(0, inplace=True)
    BSCC['Trucks Inside'] = BSCC['Trucks Inside'].astype('Int64').astype(object)
    BSCC['Trucks Awaiting Weigh In'] = BSCC['Trucks Awaiting Weigh In'].astype('Int64').astype(object)
    BSCC['Trucks Departed'] = BSCC['Trucks Departed'].astype('Int64').astype(object)
    BSCC = BSCC.replace(0, '')

    #############################################################################################################################
    customers_apcc_detail =  df[(df['weighout_datetime'].dt.day == datetime.today().day) & pd.notna(df['weighout_datetime']) & (df['tedlr_plant'] == '2000')]
    customers_apcc_bagged = customers_apcc_detail[customers_apcc_detail['tedlr_division']=='01'].groupby(['tedlr_customer_name', 'tedlr_material_description'])['tedlr_planned_qty'].sum().reset_index(name='Qty Bagged')

    customers_apcc_bagged.rename(columns={'tedlr_material_description': 'Bagged Material Description'}, inplace=True)

    customers_apcc_bulk = customers_apcc_detail[customers_apcc_detail['tedlr_division']=='02'].groupby(['tedlr_customer_name', 'tedlr_material_description'])['tedlr_net_weight'].sum().reset_index(name='Qty Bulk')
    customers_apcc_bulk.rename(columns={'tedlr_material_description': 'Bulk Material Description'}, inplace=True)


    customers_apcc = pd.merge(customers_apcc_bagged, customers_apcc_bulk, on='tedlr_customer_name', how='outer')
    customers_apcc.rename(columns={'tedlr_customer_name': 'Customer Name'}, inplace=True)

    customers_apcc['Qty Bagged'] = pd.to_numeric(customers_apcc['Qty Bagged'], errors='coerce')
    customers_apcc['Qty Bagged'].fillna(0, inplace=True)
    customers_apcc['Qty Bagged'] = customers_apcc['Qty Bagged'].astype('Int64').astype(object)
    customers_apcc = customers_apcc.replace(0, '')
    customers_apcc.fillna('', inplace=True)


    #############################################################################################################################
    customers_bscc_detail =  df[(df['weighout_datetime'].dt.day == datetime.today().day) & pd.notna(df['weighout_datetime']) & (df['tedlr_plant'] == '1000')]
    customers_bscc_bagged = customers_bscc_detail[customers_bscc_detail['tedlr_division']=='01'].groupby(['tedlr_customer_name', 'tedlr_material_description'])['tedlr_planned_qty'].sum().reset_index(name='Qty Bagged')
    customers_bscc_bagged.rename(columns={'tedlr_material_description': 'Bagged Material Description'}, inplace=True)

    customers_bscc_bulk = customers_bscc_detail[customers_bscc_detail['tedlr_division']=='02'].groupby(['tedlr_customer_name', 'tedlr_material_description'])['tedlr_net_weight'].sum().reset_index(name='Qty Bulk')
    customers_bscc_bulk.rename(columns={'tedlr_material_description': 'Bulk Material Description'}, inplace=True)

    customers_bscc = pd.merge(customers_bscc_bagged, customers_bscc_bulk, on='tedlr_customer_name', how='outer')
    customers_bscc.rename(columns={'tedlr_customer_name': 'Customer Name'}, inplace=True)

    customers_bscc['Qty Bagged'] = pd.to_numeric(customers_bscc['Qty Bagged'], errors='coerce')
    customers_bscc['Qty Bagged'].fillna(0, inplace=True)
    customers_bscc['Qty Bagged'] = customers_bscc['Qty Bagged'].astype('Int64').astype(object)
    customers_bscc = customers_bscc.replace(0, '')
    customers_bscc.fillna('', inplace=True)

    #############################################################################################################################
    # packer performance
    packer_apcc_detail =  df[(df['weighout_datetime'].dt.day == datetime.today().day) & pd.notna(df['weighout_datetime']) & (df['tedlr_plant'] == '2000')]
    packer_apcc = packer_apcc_detail.groupby('tedlr_packerid')['tedlr_planned_qty'].sum().reset_index(name='Qty')
    packer_apcc.rename(columns={'tedlr_packerid': 'Packer id'}, inplace=True)

    packer_bscc_detail =  df[(df['weighout_datetime'].dt.day == datetime.today().day) & pd.notna(df['weighout_datetime']) & (df['tedlr_plant'] == '1000')]
    packer_bscc = packer_bscc_detail.groupby('tedlr_packerid')['tedlr_planned_qty'].sum().reset_index(name='Qty')
    packer_bscc.rename(columns={'tedlr_packerid': 'Packer id'}, inplace=True)

    ##########################################################################################################################
    #Consolidated.fillna('', inplace=True)
    APCC.fillna('', inplace=True)
    BSCC.fillna('', inplace=True)
    customers_apcc.fillna('', inplace=True)
    customers_bscc.fillna('', inplace=True)
    packer_apcc.fillna('', inplace=True)
    packer_bscc.fillna('', inplace=True)

    # Assume df_all, df_apcc, and df_bscc are your DataFrames

    # Convert DataFrames to HTML strings
    html_all = Consolidated.to_html(index=False)
    html_apcc = APCC.to_html(index=False)
    html_bscc = BSCC.to_html(index=False)
    html_customers_apcc = customers_apcc.to_html(index=False)
    html_customers_bscc = customers_bscc.to_html(index=False)
    packer_apcc_html = packer_apcc.to_html(index=False)
    packer_bscc_html = packer_bscc.to_html(index=False)

    current_day = datetime.today().strftime('%d-%m-%Y')
    current_hour =  datetime.today().strftime('%H:%M')

    # Concatenate HTML strings with a divider for clarity
    html = f"""
    <!DOCTYPE html>
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">

    <style>
        table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px;  font-family: 'Calibri'}}
        th, td {{ border: 1px solid black; padding: 8px; text-align: center;  font-family: 'Calibri'}}
        th {{ background-color: #1C6EA4 ;  color: #FFFFFF; font-family: 'Calibri' }}
        .last-row {{ font-weight: bold; }}
       h1 {{
            text-align: center;
            font-family: 'Calibri';
        }}
       
        h2 {{
            text-align: center;
            font-family: 'Calibri';
        }}
         
    </style>

    </head>
    <body>
    <h1>Dispatch Status for {current_day} at {current_hour}</h1>
    <h2>Consolidated Dispatch Staus</h2>
    {html_all}
    <h2>APCC Dispatch Status</h2>
    {html_apcc}
    <h2>BSCC Dispatch Status</h2>
    {html_bscc}

    <h2>APCC Customers</h2>
    {html_customers_apcc}

    <h2>BSCC Customers </h2>
    {html_customers_bscc}

    <h2>APCC Packer </h2>
    {packer_apcc_html}

    <h2>BSCC Packer </h2>
    {packer_bscc_html}
     
    </body>
    </html>
    """


    
    return html

history = 8
html_report = generate_dispatch_status(history)

# Save the HTML to a file   
file_path = 'C:\\Users\\mabdelwahab\\Dispatch\\combined_report.html'  # Specify your file path
with open(file_path, 'w' , encoding='utf-8-sig') as f:
    f.write(html_report)

print("HTML file saved successfully.")























