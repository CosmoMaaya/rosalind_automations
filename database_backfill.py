from DataAnalysisProcessor import DataAnalysisProcessor, DbImporter, FILEPATHS

from datetime import datetime, timedelta
import pymysql

config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'pwd': 'Cosmo-6068',
    'db': 'td_data'
}

FILETYPES = {
    # "TradeActivity": "excel",
    # "Balances": "excel",
    # "Positions": "excel",
    "TD Execution Report": "excel"
}

current_date = "2020-01-14"
end_date = "2020-02-02"

dbImporter = DbImporter(**config)


# x = DataAnalysisProcessor("TD Execution Report", "excel", current_date, end_date)
# print(x.read("Sheet2"))

while current_date <= end_date:
    print("Processing {}".format(current_date))
    cdate = datetime.strptime(current_date, "%Y-%m-%d")

    for file, file_type in FILETYPES.items():
        print("Processing {}".format(file))
        processor = DataAnalysisProcessor(file, current_date, current_date)
        df = processor.read("Sheet1")
        processor.write_csv(df, current_date)

        csv_path = FILEPATHS[file] + file + "_" + datetime.strftime(cdate, "%m-%d-%Y") + ".csv"
        print(csv_path)
        dbImporter.load_csv(csv_path, file)

    cdate = cdate + timedelta(days=1)
    current_date = datetime.strftime(cdate, "%Y-%m-%d")

dbImporter.close()

assert ("Date	Ticker	Order	Shares	Price	Comm	Description	Total Cost (Comm not included)	Put/Call	Currency	Total Cash Change (Comm included)	As Of Date	Broker" ==
"Date	Ticker	Order	Shares	Price	Comm	Description	Total Cost (Comm not included)	Put/Call	Currency	Total Cash Change (Comm included)	As Of Date	Broker")
