import pandas as pd
import pymysql
import re
from datetime import datetime, timedelta

SPECIAL_DATE_FORMAT = ["TDER origin", "TD Average Price Report"]

FILEPATHS = {
    "TradeActivity": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                     "Data Analytics/Trade Reports/TradeActivity_{date}.xls",
    "TD Execution Report": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                           "Data Analytics/Trade Execution File/TD Execution Report_{date}.xlsx",
    "Balances": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                "Data Analytics/Account Balances/Balances_{date}.xls",
    "Positions": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                 "Data Analytics/Positions/Positions_{date}.xls",
    "TDER origin": "C:/Users/Bloomberg/Downloads/TD Execution Report{date}.csv",
    "Accruals": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                "Data Analytics/Accruals/Accruals_{date}.xls",
    "LoanFees": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                "Data Analytics/LoanFees/LoanFees_{date}.xls",
    "Finance": "C:/Users/Bloomberg/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/"
                "Data Analytics/Finance/Finance_{date}.xls",
}

FORMAT_REMAP = {
    ".xlsx": ".csv",
    ".xls": ".csv",
    ".xlsm": ".csv"
}

FILECOLUMNS = {
    "TradeActivity": ["Date", "Acct ID", "Acct Type", "Trade Date", "As Of Date", "Settle Date", "Trade Type", "Cusip",
                      "Ticker", "Description", "Ccy", "Qty", "Avg Price", "Comm Amt", "Net Ccy", "Net Amt", "Brk Num"],
    "TD Execution Report": ["Date", "Ticker", "Order", "Shares", "Price", "Comm", "SEC Fee", "Description",
                            "Total Cost (Comm not included)", "Put/Call", "Currency",
                            "Total Cash Change (Comm included)", "As Of Date", "Broker"],
    "Balances": ["Date", "Acct ID", "Acct Type", "Ccy", "T/D Balance", "S/D Balance", "Total Eqty", "Market Value"],
    "Positions": ["Date", "Acct ID", "Acct Type", "ISM Code", "Ticker", "CUSIP", "Description", "Ccy", "T/D Qty",
                  "Safekeep Qty", "S/D Qty", "Mkt Price", "Mkt Value"],
    "TDER origin": ["Exec Date", "User ID", "Trade Date", "Settlement Date", "Symbol", "Bloomberg Symbol", "RIC",
                    "CUSIP", "Sedol",	"ISIN",	"Description", "Product", "Execution Date Time", "Side",
                    "Total Quantity", "Execution Price", "Account Number", "Account Alias", "Exchange",
                    "Execution Broker", "Underlier", "Put/Call", "Strike Price", "Option Expiration Date", "Open/Close"],
    # "TD Average Price Report": ["Exec Date", "Trade Date", "Settlement Date", "Symbol", "Bloomberg Symbol", "RIC",
    #                             "CUSIP", "Sedol", "ISIN", "Description", "Product", "Side", "Total Quantity",
    #                             "Average Price", "Allocation Account Number", "Allocation Account Alias",
    #                             "Execution Broker", "Underlier", "Put/Call", "Strike Price", "Expiration Date",
    #                             "Open/Close"],
    "Accruals": ["Acct ID", "Acct Name", "Acct Type", "ISM Code", "CUSIP", "Description", "Cpn Rate", "Maturity",
                 "Last Cpn", "Next Cpn", "Qty", "Days Accrued", "Accrual"],
    "LoanFees": ["Acct ID", "Acct Name", "Ccy", "Ticker", "Cusip", "Description", "Open Date", "Price", "Quantity", "Rate"],
    "Finance": ["Acct ID", "Acct Name", "CCY", "Date", "Estimated S/D Balance", "Rate (%)", "Interest"]

}

COLUMNTYPE = {
    "Acct ID": "VARCHAR(16)",
    "Acct Type": "VARCHAR(16)",
    "ISM Code": "VARCHAR(16)",
    "Ticker": "VARCHAR(32)",
    "CUSIP": "VARCHAR(16)",
    "Description": "VARCHAR(255)",
    "Ccy": "VARCHAR(16)",
    "T/D Qty": "INT",
    "Safekeep Qty": "INT",
    "S/D Qty": "INT",
    "Mkt Price": "DECIMAL(16,5)",
    "Mkt Value": "DECIMAL(16,5)",
    "T/D Balance": "DECIMAL(16,5)",
    "S/D Balance": "DECIMAL(16,5)",
    "Total Eqty": "DECIMAL(16,5)",
    "Market Value": "DECIMAL(16,5)",
    "Trade Date": "DATE",
    "As Of Date": "DATE",
    "Settle Date": "DATE",
    "Date": "DATE",
    "Trade Type": "VARCHAR(16)",
    "Cusip": "VARCHAR(16)",
    "Qty": "INT",
    "Avg Price": "DECIMAL(10,6)",
    "Comm Amt": "DECIMAL(16,6)",
    "Net Ccy": "VARCHAR(16)",
    "Net Amt": "DECIMAL(16,6)",
    "Brk Num": "VARCHAR(16)",
    "Order": "VARCHAR(16)",
    "Shares": "INT",
    "Price": "DECIMAL(10,6)",
    "Comm": "DECIMAL(10,8)",
    "Total Cost (Comm not included)": "DECIMAL(16,6)",
    "Put/Call": "VARCHAR(16)",
    "Currency": "VARCHAR(16)",
    "Total Cash Change (Comm included)": "DECIMAL(16,6)",
    "Broker": "VARCHAR(16)",
    "SEC Fee": "DECIMAL(11,10)",

    #TDER columns
    "Settlement Date": "DATE",
    "Exchange": "INT",
    "Account Number": "VARCHAR(16)",
    "Side": "VARCHAR(16)",
    "Execution Broker": "VARCHAR(16)",
    "Sedol": "VARCHAR(16)",
    "Option Expiration Date": "DATE",
    "Execution Date Time": "TIME",
    "User ID": "VARCHAR(16)",
    "Execution Price": "DECIMAL(8, 5)",
    "Account Alias": "VARCHAR(16)",
    "Product": "VARCHAR(16)",
    "Underlier": "VARCHAR(16)",
    "ISIN": "VARCHAR(16)",
    "Bloomberg Symbol": "VARCHAR(32)",
    "Total Quantity": "INT",
    "Symbol": "VARCHAR(32)",
    "Strike Price": "DECIMAL(10,5)",
    "RIC": "VARCHAR(32)",
    "Exec Date": "DATE",
    "Open/Close": "VARCHAR(16)",

    # Accrual
    "Acct Name": "VARCHAR(64)",
    "Cpn Rate": "INT",
    "Maturity": "DATE",
    "Last Cpn": "DATE",
    "Next Cpn": "DATE",
    "Days Accrued": "INT",
    "Accrual": "DECIMAL(12,4)",

    # LoanFees
    "Quantity": "INT",
    "Rate": "DECIMAL(10,5)",
    "Open Date": "DATE",

    # Finance
    "CCY": "VARCHAR(16)",
    "Interest": "DECIMAL(10,5)",
    "Estimated S/D Balance": "DECIMAL(15,5)",
    "Rate (%)": "DECIMAL(10,5)"
}


class DataAnalysisProcessorError(RuntimeError):
    pass


class DataAnalysisProcessor:
    """
    Used to read files as dataframes. Files are from TD ftp server as well as REDI file. Currently would also allow user
    to write dataframes into csv files.

    Attributes:
        file: String of name of the file that is going to be processed, must be listed in both dict above
        file_type: String of the type of files you want to read, csv or excel
        from_date: String of date user wants to read from, in format of 'yyyy-mm-dd‘
        end_date: SString of date user wants to read to, in format of 'yyyy-mm-dd
    """
    def __init__(self, file, from_date, end_date):
        if file not in FILEPATHS.keys():
            raise DataAnalysisProcessorError("Wrong File {}, please refer to \"FILEPATHS\" to see valid inputs"
                                             .format(file))
        self.file = file
        self.from_date = datetime.strptime(from_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.path = FILEPATHS[file]

    def __read_single(self, date, sheet_name="Sheet0"):
        """
        read a single day file and add date column to df. For excel there are three possible file extensions.
        PRIVATE.

        :param date: date of file in datetime.
        :return: dataframe
        """
        if self.file in SPECIAL_DATE_FORMAT:
            date = datetime.strftime(date, "%Y%m%d")
        else:
            date = datetime.strftime(date, "%m-%d-%Y")

        if type == "csv":
            df = pd.read_csv(self.path.format(date=date))
        else:
            try:
                df = pd.read_excel(self.path.format(date=date), sheet_name=sheet_name)
            except FileNotFoundError:
                print("Cannot find file {}".format(FILEPATHS[self.file].format(date=date)))
                return pd.DataFrame(columns=FILECOLUMNS[self.file])

        df['Date'] = date
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        df = df[FILECOLUMNS[self.file]]
        return df

    def read(self, sheet_name="Sheet0"):
        current_date = self.from_date
        dfs = []
        while current_date <= self.end_date:
            dfs.append(self.__read_single(current_date, sheet_name))
            current_date = current_date + timedelta(days=1)

        return pd.concat(dfs)

    def write_csv(self, df, date, path=None):
        if self.file in SPECIAL_DATE_FORMAT:
            date = datetime.strftime(datetime.strptime(date, "%Y-%m-%d"), "%Y%m%d")
        else:
            date = datetime.strftime(datetime.strptime(date, "%Y-%m-%d"), "%m-%d-%Y")
        if path is None:
            path = re.sub(r'\.xls[x,m]?$', '.csv', self.path.format(date=date))
        df.to_csv(path, index=False)


class DbImporter:
    def __init__(self, host, user, pwd, db, port):
        self.db = db
        self.conn = pymysql.connect(
            host=host,
            user=user,
            passwd=pwd,
            db=db,
            port=port,
            local_infile=True
        )
        self.cur = self.conn.cursor()

    def load_csv(self, csv_file_path, table_name):
        if table_name not in FILEPATHS.keys():
            raise DataAnalysisProcessorError("Wrong Table {}, please refer to \"FILEPATHS\" to see valid inputs"
                                             .format(table_name))
        # open csv file, read the first line to get all the columns
        columns = ""

        for column in FILECOLUMNS[table_name]:
            columns = columns + "`{column}` {datetype}, ".format(column=column, datetype=COLUMNTYPE.get(column, "VARCHAR(255)"))

        columns = columns[:-2]
        create_sql = "CREATE TABLE IF NOT EXISTS `{table_name}` (" \
                     "{columns}" \
                     ");".format(table_name=table_name, columns=columns)
        data_sql = "LOAD DATA LOCAL INFILE '{csv_file_path}' INTO TABLE {db}.`{table_name}` " \
                   "FIELDS TERMINATED BY ',' " \
                   "LINES TERMINATED BY '\\r' " \
                   "IGNORE 1 LINES;".format(csv_file_path=csv_file_path, db=self.db, table_name=table_name)
        print(create_sql)
        print(data_sql)
        self.cur.execute(create_sql)
        self.cur.execute(data_sql)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


columns = []
for x in FILECOLUMNS.values():
    columns = columns + x
#
print(set(COLUMNTYPE.keys()))
print(set(columns))
print(set(COLUMNTYPE.keys()) - set(columns))
print(set(set(columns) - COLUMNTYPE.keys()))
# #
# assert(set(COLUMNTYPE.keys()) == set(columns))
#
# processor = DataAnalysisProcessor("Positions", "excel", "2020-01-01", "2020-01-01")
# df = processor.read()
# print(df)
# processor.write_csv(df, "2020-01-01")
# position_importer = DbImporter("127.0.0.1", "root", "Cosmo-6068", "test_edb", 3306)
# position_importer.load_csv(FILEPATHS["Positions"] + "Positions_01-01-2020.csv", "Positions")
#
# # DataAnalysisProcessor("a", "a", "a", "a").read_single("a")
#
# # pd.read_excel("sd")


