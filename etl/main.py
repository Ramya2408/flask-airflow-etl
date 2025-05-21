import json
import psycopg2
import pandas as pd
from configparser import ConfigParser

def load_config():
    cp = ConfigParser()
    cp.read('../config/config.ini')
    return cp

def compute_vat(df, rates):
    # map item_type → vat rate
    df['vat_rate'] = df['item_type'].map(rates)
    df['net_value'] = df['price'] / (1 + df['vat_rate'])
    return df

def write_to_postgres(df, conn_params):
    cols = ','.join(df.columns)
    vals = ','.join(['%s'] * len(df.columns))
    insert_sql = f"INSERT INTO orders ({cols}) VALUES ({vals})"
    with psycopg2.connect(**conn_params) as conn:
        cur = conn.cursor()
        for row in df.itertuples(index=False, name=None):
            cur.execute(insert_sql, row)
        conn.commit()

def main():
    cfg = load_config()
    # load orders JSON
    path = cfg['etl']['orders_path']
    data = pd.read_json(path)
    # compute VAT
    rates = {k: float(v) for k,v in 
             [r.split(':') for r in cfg['etl']['vat_rates'].split(',')]}
    processed = compute_vat(data, rates)
    # write to Postgres
    pg = dict(cfg['postgres'])
    write_to_postgres(processed, pg)

if __name__ == "__main__":
    main()
