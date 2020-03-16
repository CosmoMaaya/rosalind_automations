import pandas as pd
import unittest

from pandas.testing import assert_frame_equal
from RediProcessor import RediProcessor, USER_ORIGIN

SAVE_PATH = USER_ORIGIN + "/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/" \
            "Data Analytics/Automations/Test File/Execution_Report.xlsx"

FROM_PATH = USER_ORIGIN + "/Dropbox (Rosalind Advisors)/_ROSALIND Operations - SHARED/" \
            "Data Analytics/Automations/Test File/TD Execution ReportTDEX_Prx_Template_2016091.csv"


class MyTestCase(unittest.TestCase):
    def test_something(self):
        processor = RediProcessor(FROM_PATH, SAVE_PATH)
        processor.redi_process()

        result_df = pd.read_excel(SAVE_PATH)
        correct_df = pd.read_excel("Test File/TD Execution Report_01-10-2020.xlsx", sheet_name="Sheet2")
        # print(correct_df)
        assert_frame_equal(result_df, correct_df)

if __name__ == '__main__':
    unittest.main()
