import os, os.path
import re

INPUT_DIR = 'ExampleOutput'
OUTPUT_DIR = 'BillAnalysisTextFixed'

def main():
    for root, _, files in os.walk(INPUT_DIR):
        file_num = 0
        for f in files:
            if '.txt' in f:
                try:
                    in_path = os.path.join(root, f)
                    f_input = open(in_path, 'r')
                    text = f_input.read()

                    bill_name_reg = r"[A-Z]{2,4}(\d{0,1})\s+\d{1,4}"
                    match = re.search(bill_name_reg, text)
                    bill_name = match.group(0)

                    date_reg = r'\d\d-\d\d-\d\d\d\d'
                    match = re.search(date_reg, f)
                    date = match.group(0)

                    bill_name = bill_name.replace(' ', '')
                    out_f_name = bill_name + '_' + date + '.txt'
                    out_path = OUTPUT_DIR + '/' + out_f_name
                    f_output = open(out_path, 'r')

                    f_output.write(text)
                    file_num += 1

                except AttributeError:
                    print('No bill name match')

    print('Num files: ', file_num)



if __name__ == '__main__':
    main()
