import SupportFinder
import re

def GetLine(line):
    try:
        # out = str(line, "utf-8")
        # completely forgot why I was doing this
        out = line
    except UnicodeDecodeError:
        print("Decode Error")
        print(line)
        exit()
    return out


def compareFiles(file1, file2):
    with open(file1, "r") as f1:
        text1 = f1.read().splitlines()
        f1.close()
    with open(file2, "r") as f2:
        text2 = f2.read().splitlines()
        f2.close()

    for i in range(0, max(len(text1), len(text2))):
        try:    
            line1 = GetLine(text1[i])
        except IndexError:
            print(file2 + " has more lines")
            exit()
        try:
            line2 = GetLine(text2[i])
        except IndexError:
            print(file1 + " has more lines")
            exit()

        line2 = line2.strip()

        rgx = r'"(.*)"'
        match = re.search(rgx, line1)
        line1 = match.group(1)

        match = re.search(rgx, line2)    
        if match:
          line2 = match.group(1)

        if line1 != line2:
            print("Files do not match")
            print(line1)
            print(line2)
            exit()


kSupportFile = "support.csv"
kOpposeFile = "opposition.csv"
kDirPath = 'TestCases/'

tests = [   
            # 1: Case 1
            {"file" : "C1.txt", 
             "support" : "C1Sup.csv", 
             "opposition" : "C1Opp.csv"},

            # 2: Case 2
             {"file" : "C2.txt",
              "support" : "C2Sup.csv",
              "opposition" : "C2Opp.csv"},

            # 3: Case 2 -Yea you done screwed this up
             {"file" : "C3.txt",
              "support" : "C3Sup.csv",
              "opposition" : "C3Opp.csv"},

            # 4: Case 2
             {"file" : "201520160ACR46_07-14-2015.txt",
              "support" : "ACR46-7-14-15Sup.csv",
              "opposition" : "ACR46-7-14-15Opp.csv"},

            # 5: Case 1
             {"file" : "201520160ACR46_04-06-2015.txt",
              "support" : "ACR46-4-6-15Sup.csv",
              "opposition" : "ACR46-4-6-15Opp.csv"},

            # 6: Case 1
             {"file" : "201520160SCR46_05-21-2015.txt",
              "support" : "SCR46-5-22-15Sup.csv",
              "opposition" : "SCR46-5-22-15Opp.csv"},

            # 7: Case 2
             {"file" : "201520160AB10_07-06-2015.txt",
              "support" : "AB_10_07-06-2015_Support.csv",
              "opposition" : "AB_10_07-06-2015_Oppose.csv"},

            # 8: Case 2
             {"file" : "201520160AB10_08-28-2015.txt",
              "support" : "AB_10_08-28-2015_Support.csv",
              "opposition" : "AB_10_08-28-2015_Oppose.csv"},

            # 9: Case 2
             {"file" : "201520160AB1008_06-12-2015.txt",
              "support" : "AB_1008_06-12-2015_Support.csv",
              "opposition" : "AB_1008_06-12-2015_Oppose.csv"},

            # 10: Case 2
             {"file" : "201520160AB1004_07-07-2015.txt",
              "support" : "AB_1004_07-07-2015_Support.csv",
              "opposition" : "AB_1004_07-07-2015_Oppose.csv"},

            # 11: Case 2
             {"file" : "201520160AB1015_06-04-2015.txt",
              "support" : "AB_1015_06-04-2015_Support.csv",
              "opposition" : "AB_1015_06-04-2015_Oppose.csv"},

            # 12: Case 1 ## pretty sure this is just unsolvable
             # {"file" : "AB_103_04-20-2015.txt",
             #  "support" : "AB_103_04-20-2015_Support.csv",
             #  "opposition" : "AB_103_04-20-2015_Oppose.csv"}

            # 12: Case 2
             {"file" : "201520160AB1034_07-10-2015.txt",
              "support" : "AB_1034_07-10-2015_Support.csv",
              "opposition" : "AB_1034_07-10-2015_Oppose.csv"},

            # 13: Case 1
             {"file" : "201520160AB1059_04-10-2015.txt",
              "support" : "AB_1059_04-10-2015_Support.csv",
              "opposition" : "AB_1059_04-10-2015_Oppose.csv"},

            # 14: Case 1
             {"file" : "201520162AB15_09-01-2015.txt",
              "support" : "ABX2_15_09-01-2015_Support.csv",
              "opposition" : "ABX2_15_09-01-2015_Oppose.csv"}
        ]


testNum = 1
for test in tests:
  print("Test Number: " + str(testNum))
  testNum += 1
    # wipes both files
  open(kSupportFile, "w").close()
  open(kOpposeFile, "w").close()

  SupportFinder.BillScrape(kDirPath + test['file'], kSupportFile,
      kOpposeFile)

  compareFiles(kSupportFile, kDirPath + test["support"])
  compareFiles(kOpposeFile, kDirPath + test["opposition"])