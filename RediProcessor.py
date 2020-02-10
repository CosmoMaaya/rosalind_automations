from datetime import date, timedelta
from decimal import *
from time import strptime, strftime
import numpy as np
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', 100)
pd.set_option('display.width', None)

COLUMNS_CORRES_NAME = {"Exec Date": "Date",
                       "Symbol": "Ticker",
                       "Side": "Order",
                       "Shares": "Shares",
                       "Avg": "Price",
                       "Description": "Description",
                       "Total Cost": "Total Cost (Comm not included)",
                       "Put/Call": "Put/Call",
                       "Execution Broker": "Currency"
                       }

ARRANGED_COLS = ['Date', 'Ticker', 'Order', 'Shares', 'Price', 'Comm', 'SEC Fee', 'Description', 'Total Cost (Comm not included)',
                 'Put/Call', 'Currency', 'Total Cash Change (Comm included)', 'As Of Date', 'Broker']

SAVE_PATH = "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/" \
            "Data Analytics/Trade Execution File/TD Execution Report_{date}.xlsx"

SEC_FEE_CHANGE_DATE = "2020-02-18"
SEC_FEE = 20.70 if date.today().strftime("%Y-%m-%d") < SEC_FEE_CHANGE_DATE else 22.10

# FIVE_DP = Decimal('0.00000')
# SIX_DP = Decimal('0.000000')
# TWO_DP = Decimal('0.00')


class RediProcessor:
    def __init__(self, redi_path, save_path=SAVE_PATH):
        self.redi_path = redi_path
        self.save_path = save_path

    def redi_process(self):
        df = pd.read_csv(self.redi_path)
        if df.empty:
            df = pd.DataFrame(columns=ARRANGED_COLS)
        else:
            df = self.extract_data_from_redi(df)
            df = self.rename_rearrange_col(df)
            df = self.process_data(df)

        writer = pd.ExcelWriter(self.get_save_path(), engine='xlsxwriter')
        df.to_excel(writer, sheet_name='Sheet1', index=False)
        worksheet = writer.sheets['Sheet1']
        worksheet.add_table('A1:N{}'.format(len(df) + 1 if len(df) > 0 else 2), {
            'name': 'Trade_Execution_Final_Table',
            'columns': [{'header': col_name} for col_name in ARRANGED_COLS]})
        writer.save()

    def extract_data_from_redi (self, df):
        df['Value'] = df['Total Quantity'] * df['Execution Price']
        df['Total Cost'] = df.groupby(['Symbol', 'Side'])['Value'].transform('sum')
        df['Shares'] = df.groupby(['Symbol', 'Side'])['Total Quantity'].transform('sum')
        df['Avg'] = df['Total Cost'] / df['Shares']
        final = df[COLUMNS_CORRES_NAME.keys()].drop_duplicates()
        return final

    def rename_rearrange_col(self, df):
        renamed_df = df.rename(columns=COLUMNS_CORRES_NAME).reset_index(drop=True)
        renamed_df = pd.concat([renamed_df,
                                pd.DataFrame(columns=[item for item in ARRANGED_COLS
                                                      if item not in COLUMNS_CORRES_NAME.values()])],
                               sort=False)
        rearranged_col = renamed_df[ARRANGED_COLS]
        return rearranged_col

    def process_data(self, df):
        df['Price'] = df.apply(lambda arrLike: round(arrLike['Price'], 5) if pd.isna(arrLike['Put/Call']) else round(arrLike['Price'], 6), axis=1)
        df['Date'] = pd.to_datetime(df['Date']).dt.date

        df['Order'] = df['Order'].transform(lambda x: self.__order_change(x))
        df['Comm'] = df.apply(self.cal_comm, axis=1)
        df['Shares'] = df.apply(lambda arrLike: arrLike['Shares'] * 100
                                if not pd.isna(arrLike['Put/Call'])
                                else arrLike['Shares']
                                , axis=1)
        df['Currency'] = df.apply(lambda arrLike: "CAD" if arrLike['Currency'] == "TDCA" else "USD", axis=1)
        df['SEC Fee'] = df.apply(lambda arrLike:
                                 (arrLike['Price'] * SEC_FEE / 1000000)
                                 if arrLike['Currency'] == "USD" and "SOLD" in arrLike['Order']
                                 else 0, axis=1)
        df['Total Cost (Comm not included)'] = df.apply(lambda arrLike:
                                                       (-1) * arrLike['Total Cost (Comm not included)']
                                                       if arrLike['Order'] == "BOUGHT"
                                                       else arrLike['Total Cost (Comm not included)'], axis=1)
        df['Total Cost (Comm not included)'] = df.apply(lambda arrLike:
                                                       arrLike['Total Cost (Comm not included)'] * 100
                                                       if not pd.isna(arrLike['Put/Call'])
                                                       else arrLike['Total Cost (Comm not included)'], axis=1)
        df['Total Cash Change (Comm included)'] = df.apply(lambda arrLike:
                                                        (-1) * (arrLike['Price'] + arrLike['Comm']) * arrLike['Shares']
                                                        if arrLike['Order'] == "BOUGHT"
                                                        else (arrLike['Price'] - arrLike['Comm']) * arrLike['Shares'] - float(Decimal(arrLike['SEC Fee'] * arrLike['Shares']).quantize(Decimal('.01'), rounding=ROUND_UP)),
                                                        axis=1)

        print(df)
        return df

    def __order_change(self, x):
        if x == "Sell":
            return "SOLD"
        elif x == "Short Sell":
            return "SHORT SOLD"
        else:
            return "BOUGHT"

    def cal_comm (self, arrLike):
        option = arrLike['Put/Call']
        side = arrLike['Order']
        price = arrLike['Price']

        if pd.isna(option):
            if side == "BOUGHT":
                if price >= 1:
                    return 0.008
                else:
                    return 0.005
            else:
                if price >= 1:
                    return 0.008
                else:
                    return 0.005
        else:
            if side == "BOUGHT":
                if price >= 1:
                    return 0.01
                else:
                    return 0.007
            else:
                if price >= 1:
                    return 0.01
                else:
                    return 0.007

    def get_save_path(self):
        todayWeekday = date.today().weekday()
        if 1 <= todayWeekday <= 5:
            return self.save_path.format(date=(date.today()-timedelta(days=1)).strftime("%m-%d-%Y"))
        elif todayWeekday == 0:
            return self.save_path.format(date=(date.today()-timedelta(days=3)).strftime("%m-%d-%Y"))
        else:
            return self.save_path.format(date=(date.today()-timedelta(days=2)).strftime("%m-%d-%Y"))

# processor = RediProcessor("TD Execution ReportTDEX_Prx_Template_2016091.csv")
# r"C:\Users\Bloomberg\Dropbox (Rosalind Advisors)\_ROSALIND Operations - SHARED\
#         Data Analytics\Trade Execution File\TD Execution ReportTDEX_Prx_Template_2016091.csv")

# processor.redi_process()
