import pandas as pd
from pathlib import Path
import yaml

CONFIG_PATH = Path("config.yaml")
DATA_DIR = Path("data")
OUT_DIR = Path("out")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def load_config(path=CONFIG_PATH):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def normalize_prices(df, config):
    # expects columns: ['date','symbol','delivery_month','price','units','currency']
    df = df.copy()
    # FX step is skipped (assume USD inputs for PoC)
    def to_usd_per_bbl(row):
        u = row['units'].lower()
        p = row['price']
        if u in ['usd/bbl', 'usd per bbl', 'usd per barrel']:
            return p
        elif u in ['usd/gal', 'usd per gallon']:
            return p * 42.0
        elif u in ['usd/mt', 'usd/ton', 'usd/tonne']:
            # use gasoil factor from config if symbol looks like gasoil
            bbl_per_mt = config['universe']['products'][2]['bbl_per_metric_ton']
            return p / bbl_per_mt
        else:
            raise ValueError(f"Unknown units: {row['units']}")
    df['price_usd_per_bbl'] = df.apply(to_usd_per_bbl, axis=1)
    return df

def compute_cracks(data):
    # data: dict of DataFrames keyed by logical name, all normalized to USD/bbl and same delivery_month
    # expects columns: ['date','delivery_month','price_usd_per_bbl']
    merged = None
    for name, d in data.items():
        d = d[['date','delivery_month','price_usd_per_bbl']].rename(columns={'price_usd_per_bbl': name})
        merged = d if merged is None else merged.merge(d, on=['date','delivery_month'], how='outer')
    merged = merged.sort_values(['date','delivery_month']).dropna()
    merged['crack_1_1_gasoline'] = merged['RBOB'] - merged['Crude']
    merged['crack_1_1_distillate'] = merged['Distillate'] - merged['Crude']
    merged['crack_3_2_1'] = (2*merged['RBOB'] + merged['Distillate'] - 3*merged['Crude'])/3.0
    return merged

def example():
    # Load dummy CSVs (user should replace with real downloads)
    # Each CSV must have: date, symbol, delivery_month, price, units, currency
    crude = pd.read_csv(DATA_DIR/'CL_settlements.csv', parse_dates=['date'])
    rb = pd.read_csv(DATA_DIR/'RB_settlements.csv', parse_dates=['date'])
    ho = pd.read_csv(DATA_DIR/'HO_settlements.csv', parse_dates=['date'])
    cfg = load_config()
    crude_n = normalize_prices(crude, cfg).rename(columns={'price_usd_per_bbl': 'Crude'})
    rb_n = normalize_prices(rb, cfg).rename(columns={'price_usd_per_bbl': 'RBOB'})
    ho_n = normalize_prices(ho, cfg).rename(columns={'price_usd_per_bbl': 'Distillate'})
    merged = compute_cracks({'Crude': crude_n, 'RBOB': rb_n, 'Distillate': ho_n})
    merged.to_csv(OUT_DIR/'cracks_by_month.csv', index=False)
    print("Saved:", OUT_DIR/'cracks_by_month.csv')

if __name__ == "__main__":
    example()