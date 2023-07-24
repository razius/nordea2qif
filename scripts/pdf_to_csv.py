from decimal import Decimal
from datetime import datetime
import os
import csv
from dateutil.parser import parse

ARCHIVE_FOLDER = 'exports/nordea/archive/'
OUTPUT_FOLDER = 'exports/nordea/'

def convert(account):
    filenames = sorted([i for i in os.listdir(f'{ARCHIVE_FOLDER}/{account}/') if i.endswith('.txt')])
    csv_rows = [
        ['Bogført', 'Tekst', 'Rentedato', 'Beløb', 'Saldo']
    ]
    for filename in filenames:
        full_filename = f'{ARCHIVE_FOLDER}/{account}/{filename}'
        print(f'Processing: {full_filename}')
        with open(full_filename) as f:
            csv_reader = csv.reader(f, delimiter='|')
            report_date = None
            rows = [i for i in list(csv_reader) if i and '---+----' not in i[0]]
            for r in rows:
                r = [i.strip() for i in r if i.strip()]
                if len(r) == 1 and r[0].endswith('Dato') and not report_date:
                    report_date = datetime.strptime(r[0].replace('Dato', '').strip(), '%d.%m.%Y')
                if len(r) >= 3 and r[0] != 'Dato' and all([
                        'Overført fra forrige side' not in r[1],
                        'Overført fra udskrift' not in r[1],
                        'Overført til næste side' not in r[1],
                ]):
                    # Try to figure out row date based on report date as the year
                    # is missing from the export.
                    day, month = [int(i) for i in r[0].split('.')]
                    year = report_date.date().year
                    if month > report_date.date().month:
                        year = report_date.date().year - 1
                    parsed_date = datetime(year=year, month=month, day=day).date()
                    row = [parsed_date, r[1]]
                    remainder_columns = [i.strip() for i in r[2].split()]
                    # Sometimes the balance total is missing
                    if len(remainder_columns) == 2:
                        amount = Decimal(remainder_columns[-1].replace('.', '').replace(',', '.'))
                        # previous_row = rows[index - 1]
                        previous_row = csv_rows[-1]
                        previous_balance = Decimal(previous_row[-1].split()[-1].replace('.', '').replace(',', '.'))
                        remainder_columns.append(str(previous_balance + amount).replace('.', ','))
                    row.extend(remainder_columns)
                    csv_rows.append(row)


    with open(f'{OUTPUT_FOLDER}/{account}/converted_from_txt.csv', 'w') as f:
        csv.writer(f, delimiter=';').writerows(csv_rows)

def main():
    for account in ['savings', 'current']:
        convert(account)
