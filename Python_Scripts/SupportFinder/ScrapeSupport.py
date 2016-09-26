import SupportFinder
import os

# billDir = "BillAnalysisText/"
# billDir = "ExampleOutput/"
billDir = "BillAnalysisTextFixed/"
files = os.listdir(billDir)

case_1 = 0
case_2 = 0
case_3 = 0
num_empty = 0
for fileName in files:
    if fileName[-4:] == ".txt" and fileName != 'FinishedBills.txt':
        # print fileName
        bill = billDir + fileName
        supportOut = bill[:-4] + "_Support.csv"
        opposeOut = bill[:-4] + "_Oppose.csv"

        cases = SupportFinder.BillScrape(bill, supportOut, opposeOut)
        case_1 += cases[0]
        case_2 += cases[1]
        case_3 += cases[2]
        num_empty += cases[3]


print('Case 1', case_1, 'Case_2', case_2, 'Case_3', case_3, 'Empty', num_empty)
