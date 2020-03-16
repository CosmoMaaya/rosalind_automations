import os, os.path
import re
import time
from datetime import timedelta, datetime
from shutil import copyfile

from DataAnalysisProcessor import DataAnalysisProcessor, DbImporter, FILEPATHS, USER_ORIGIN
from RediProcessor import RediProcessor

todayWeekday = datetime.today().weekday()

DestFolder = {
    "Positions": "Positions",
    "Balances": "Account Balances",
    "TradeActivity": "Trade Reports",
    "LoanFees": "LoanFees",
    "Accruals": "Accruals",
    "Finance": "Finance"
}

config = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'pwd': 'Cosmo-6068',
    'db': 'td_data'
}

FILETYPES = ["TradeActivity", "Balances", "Positions", "TD Execution Report", "LoanFees", "Accruals", "Finance"]

path_origin = USER_ORIGIN + "/Downloads/{file_type}_{date}.xls"
path_destination = USER_ORIGIN + "/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/" \
                   "Data Analytics/{folder}/{file_type}_{date}.xls"

REDI_PATH = USER_ORIGIN + "/Downloads/TD Execution Report{date}.csv"

SANDBOX_SAVE_PATH = USER_ORIGIN + "/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/" \
            "Data Analytics/Trade Execution File/TD Execution Report_{date}_Sandbox.xlsx"


def copyFiles(dateString):
    for fileType in DestFolder:
        if os.path.exists(path_origin.format(file_type=fileType, date=dateString)):
            copyfile(path_origin.format(file_type=fileType, date=dateString), path_destination.format(
                folder=DestFolder[fileType], file_type=fileType, date=dateString))


if 1 <= todayWeekday <= 5:
    dateString = (datetime.today() - timedelta(days=1)).strftime('%m-%d-%Y')
    copyFiles(dateString)
    processor = RediProcessor(REDI_PATH.format(date=(datetime.today() - timedelta(days=1)).strftime('%Y%m%d')))
    sandbox_porc = RediProcessor(REDI_PATH.format(date=(datetime.today() - timedelta(days=1)).strftime('%Y%m%d')),
                                 save_path=SANDBOX_SAVE_PATH)
elif todayWeekday == 0:
    dateString = (datetime.today() - timedelta(days=3)).strftime('%m-%d-%Y')
    copyFiles(dateString)
    processor = RediProcessor(REDI_PATH.format(date=(datetime.today() - timedelta(days=3)).strftime('%Y%m%d')))
    sandbox_porc = RediProcessor(REDI_PATH.format(date=(datetime.today() - timedelta(days=3)).strftime('%Y%m%d')),
                                 save_path=SANDBOX_SAVE_PATH)
else:
    dateString = (datetime.today() - timedelta(days=2)).strftime('%m-%d-%Y')
    copyFiles(dateString)
    processor = RediProcessor(REDI_PATH.format(date=(datetime.today() - timedelta(days=2)).strftime('%Y%m%d')))
    sandbox_porc = RediProcessor(REDI_PATH.format(date=(datetime.today() - timedelta(days=2)).strftime('%Y%m%d')),
                                 save_path=SANDBOX_SAVE_PATH)

processor.redi_process()
sandbox_porc.redi_process()


dbImporter = DbImporter(**config)

cdate = datetime.strptime(dateString, "%m-%d-%Y")
dateString = datetime.strftime(cdate, "%Y-%m-%d")

if 1 <= todayWeekday <= 5:
    for file in FILETYPES:
        print("Processing {}".format(file))
        processor = DataAnalysisProcessor(file, dateString, dateString)
        if file == "TD Execution Report":
            df = processor.read("Sheet1")
        else:
            df = processor.read()
        processor.write_csv(df, dateString)

        csv_path = re.sub(r'\.xls[x,m]?$', '.csv', FILEPATHS[file].format(date=datetime.strftime(cdate, "%m-%d-%Y")))
        print(csv_path)
        # dbImporter.load_csv(csv_path, file)

    dbImporter.close()

