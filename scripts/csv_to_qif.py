c!/bin/python
from datetime import datetime
import pandas
import io
import os
import csv
import quiffen
from chardet import detect
from decimal import Decimal
from dateutil.parser import parse, parserinfo
from collections import Counter

EXPORTS_DIR = 'exports/nordea'
ACCOUNTS = ['Current', 'Savings']
OUTPUT_FOLDER = 'temp'


def get_rows():
    for account in ACCOUNTS:
        for filename in sorted(os.listdir(f'{EXPORTS_DIR}/{account.lower()}')):
            print(f'Parsing {filename}')
            seen_dates = set()
            with open(f'{EXPORTS_DIR}/{account.lower()}/{filename}', mode='rb') as f:
                content = f.read()
                encoding = detect(content)
                csv_reader = csv.reader(io.StringIO(content.decode(encoding['encoding'])), delimiter=';')
                headers = next(csv_reader)
                # eliminate empty first row.
                if not headers:
                    headers = next(csv_reader)
                rows = list(csv_reader)
                if len(headers) == 5:
                    for row in rows:
                        posting_date, text, _, amount, balance = row
                        # Skip until an unseen date as sometimes a partial day export is done,
                        # transactions are missing from the day and the balance is not the same
                        # so we can't remove duplicated transactions
                        if posting_date not in seen_dates:
                            yield [posting_date, text, amount, balance, account]
                elif len(headers) == 9:
                    for row in rows:
                        posting_date, amount, _, _, _, text, balance, _, _ = row
                        if posting_date not in seen_dates:
                            yield [posting_date, text, amount, balance, account]
                else:
                    raise Exception('Unknown number of columns')
                seen_dates = {i[0] for i in rows}

def get_df(rows):
    records = []
    for posting_date, text, amount, balance, account in rows:
        formated_posting_date = None
        for f in ['%d-%m-%Y', '%d.%m.%Y', '%Y/%m/%d', '%Y-%m-%d']:
            try:
                formated_posting_date = datetime.strptime(posting_date, f)
            except ValueError:
                pass
        if not formated_posting_date:
            raise Exception(f'Unknown date format for: {posting_date}')
        amount = Decimal(amount.replace('.', '').replace(',', '.'))
        balance = Decimal(balance.replace('.', '').replace(',', '.') or 0)
        records.append([formated_posting_date, text, amount, balance, account])
    df = pandas.DataFrame.from_records(
        records, columns=['posting_date', 'text', 'amount', 'balance', 'account'],
    ).drop_duplicates().sort_values('posting_date')

    # TODO: Sort so that balance matches
    # Drop duplicates like:
    # 2021-10-10.csv:10-12-2020;Bgs From Savings                   Check 03759011820345;10-12-2020;20000,00;20824,59
    # 2021-01-27.csv:10-12-2020;Bgs From Savings;10-12-2020;20000,00;20824,59
    df = df.groupby(['posting_date', 'amount', 'balance', 'account'])['text'].apply(','.join).reset_index()
    df['text'] = df['text'].str.strip()

    return df


def write_to_homebank_format(df):
    accounts = {
        a: quiffen.Account(name=a) for a in ACCOUNTS
    }
    qif = quiffen.Qif()
    for acc in accounts.values():
        qif.add_account(acc)
    for row in df.to_records(index=False):
        acc = accounts[row[3]]
        acc.add_transaction(
            quiffen.Transaction(
                date=row[0],
                memo=f'{row[2]}: {row[-1]}',
                amount=row[1],
                cleared='X',
                # payee=get_payee(row),
                # category=get_category(row),
            ),
            header='Bank',
        )
    qif.to_qif(f'{OUTPUT_FOLDER}/nordea.qif')


def main():
    df = get_df(get_rows())
    crite_to_homebank_format(df)
