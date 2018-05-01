from pprint import PrettyPrinter
from os import listdir, getcwd, path, makedirs
import csv

pp = PrettyPrinter(indent=4)

current_dir = f'{getcwd()}'
input_dir = f'{current_dir}/input'
output_dir = f'{current_dir}/output'
output_file = f'{output_dir}/output.csv'

header_fields = set()


def cleanse_header(header: set):
    return set(str(col).replace(' (S)', '').replace(' (N)', '') for col in header)


csv_records = []

for inp_file in listdir(input_dir):
    with open(f'{input_dir}/{inp_file}') as inp_csvfile:
        csvreader = csv.DictReader(inp_csvfile, delimiter=',', quotechar='"')
        header_fields.update(csvreader.fieldnames)
        for item in csvreader:
            csv_records.append(dict(item))

if not path.exists(output_dir):
    makedirs(output_dir)

header_fields = list(header_fields)
with open(output_file, 'w') as out_csvfile:
    csvwriter = csv.DictWriter(out_csvfile, fieldnames=header_fields)

    csvwriter.writeheader()
    for item in csv_records:
        csvwriter.writerow(item)
