
import csv
import sys
import pandas as pd
import argparse

MsgType = 35
NewOrderSingle = "D"
ExecutionReport = "8"
ExecType = 150
Fill = "2"
OrdStatus = 39
Filled = "2"
OrdType = 40
Limit = "2"
CIOrdID = 11
TransactTime = 60
Symbol = 55
Side = 54
OrderQty = 38
LimitPrice = 44
AvgPx = 6
LastMkt = 30

input_fix_file = "cleaned.fix"
output_fix_file = "output_csv_file.csv"

def parse_fix_to_dict(message: str) -> dict:
    message_dict = {}
    separator = '\x01'
    tags = message.strip().split(separator)
    for tag in tags:
        if '=' in tag:
            tag, value = tag.split('=')
            if tag.isdigit():
                message_dict[int(tag)] = value
    return message_dict

parser = argparse.ArgumentParser(description="Parser")
parser.add_argument('--input_fix_file')
parser.add_argument('--output_csv_file')
args = parser.parse_args()


with open(args.input_fix_file, 'r') as f:
      lines = f.readlines()

parsed_fix = [parse_fix_to_dict(line) for line in lines if line.strip()]


new_orders = {}
filled_orders_data = []

for msg in parsed_fix:
    if msg.get(MsgType) == NewOrderSingle:
        order_id = msg.get(CIOrdID)
        if order_id:
            new_orders[order_id] = {
                'OrderID': order_id,
                'OrderTransactTime': msg.get(TransactTime),
                'Symbol': msg.get(Symbol),
                'Side': msg.get(Side),
                'OrderQty': msg.get(OrderQty),
                'LimitPrice': msg.get(LimitPrice)
            }

    elif msg.get(MsgType) == ExecutionReport:
        is_limit_fill = (
            msg.get(ExecType) == Fill and
            msg.get(OrdStatus) == Filled and
            msg.get(OrdType) == Limit
        )

        if is_limit_fill:
            order_id = msg.get(CIOrdID)
            if order_id in new_orders:
                original_order = new_orders[order_id]
                final_record = {
                    'OrderID': original_order['OrderID'],
                    'OrderTransactTime': original_order['OrderTransactTime'],
                    'ExecutionTransactTime': msg.get(TransactTime),
                    'Symbol': original_order['Symbol'],
                    'Side': original_order['Side'],
                    'OrderQty': original_order['OrderQty'],
                    'LimitPrice': original_order['LimitPrice'],
                    'AvgPx': msg.get(AvgPx),
                    'LastMkt': msg.get(LastMkt)
                }
                filled_orders_data.append(final_record)

if filled_orders_data:
    output_df = pd.DataFrame(filled_orders_data)

    column_order = [
        'OrderID', 'OrderTransactTime', 'ExecutionTransactTime', 'Symbol',
        'Side', 'OrderQty', 'LimitPrice', 'AvgPx', 'LastMkt'
    ]
    output_df = output_df[column_order]
    output_df.to_csv(output_fix_file, index=False)

import argparse
import pandas as pd
import numpy as np
import sys

def CalculateMetrics(input_file: str, output_file: str):
    try:
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"File Not Found '{input_file}'")
        sys.exit(1)

    df['OrderTransactTime'] = pd.to_datetime(df['OrderTransactTime'])
    df['ExecutionTransactTime'] = pd.to_datetime(df['ExecutionTransactTime'])
    df['LimitPrice'] = pd.to_numeric(df['LimitPrice'])
    df['AvgPx'] = pd.to_numeric(df['AvgPx'])
    df['Side'] = pd.to_numeric(df['Side'])
    df['ExecSpeedSecs'] = (df['ExecutionTransactTime'] - df['OrderTransactTime']).dt.total_seconds()

    BetterBuy = df['LimitPrice'] - df['AvgPx']
    BetterSell = df['AvgPx'] - df['LimitPrice']


    df['PriceImprovement'] = np.where(df['Side'] == 1, BetterBuy, BetterSell)


    df['PriceImprovement'] = df['PriceImprovement'].clip(lower=0)

    Metrics = df.groupby('LastMkt').agg(
        AvgPriceImprovement=('PriceImprovement', 'mean'),
        AvgExecSpeedSecs=('ExecSpeedSecs', 'mean')
    ).reset_index()

    Metrics.to_csv(output_file, index=False, float_format='%.15f')



def main():
    parser = argparse.ArgumentParser(
        description="Calculate Matrics"
    )
    parser.add_argument(
        '--input_csv_file',
        required=True,
    )
    parser.add_argument(
        '--output_metrics_file',
        required=True,
    )
    args = parser.parse_args()
    CalculateMetrics(args.input_csv_file, args.output_metrics_file)


if __name__ == "__main__":
    main()
