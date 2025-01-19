import pandas as pd
from sec_edgar_downloader import Downloader
import json
import snowflake.connector as sf
from pathlib import Path

def get_config(component="connection",config_file='config.json') -> dict:
    script_dir = Path(__file__).resolve().parent.parent
    with open(f'{script_dir}\{config_file}', 'r') as file:
        config = json.load(file)
    return config.get(component, {})


def get_sp500_tickers():
    import requests
    from bs4 import BeautifulSoup

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    
    response = requests.get(url)
    response.raise_for_status()
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "constituents"})
    df = pd.read_html(str(table))[0]
    tickers = df["Symbol"].tolist()
    cleaned_tickers = [t.replace(".", "-") for t in tickers]
    
    return cleaned_tickers


def download_10k_filings(tickers, num_tickers, amount=1, download_folder="SEC-Edgar-Data"):
    """
    Downloads 10-K filings from SEC for the given list of tickers.
    
    :param tickers: List of stock ticker symbols
    :param amount: Number of recent 10-K filings to download per ticker
    :param download_folder: Folder where downloaded filings will be stored
    """
    dl = Downloader("Continuus", "mlake@continuus.ai",download_folder=download_folder)
    
    for ticker in tickers[:num_tickers]:
        try:
            print(f"Downloading {amount} 10-K filing(s) for {ticker}...")
            dl.get(
                form="10-K",
                ticker_or_cik=ticker,
                limit=amount
            )
        except Exception as e:
            print(f"Error downloading filings for {ticker}: {e}")

def get_leaf_folder(base_path):
    """
    Traverses a nested folder structure and retrieves all files
    from the deepest (leaf) folder.
    
    Args:
        base_path (str): Path to the base directory to start the search.
    
    Returns:
        list: A list of file paths in the leaf folder.
    """
    base_path = Path(base_path)
    
    for folder in base_path.rglob('*'):
        if folder.is_dir() and not any(sub.is_dir() for sub in folder.iterdir()):
            return str(folder)
    


def upload_files(path,stage_name, num_tickers = 1):

    script_dir = Path(__file__).resolve().parent.parent
    dir = f'{script_dir}\\10_k_filings'
    download_10k_filings(get_sp500_tickers(), num_tickers, amount=1, download_folder=dir)
    conn = sf.connect(**get_config())
    try: 
        cur = conn.cursor()
        file_paths = [str(subdir) for subdir in Path(f'{dir}\{path}').iterdir() if subdir.is_dir()]
        for fp in file_paths:
            ticker = fp.split('\\')[-1]
            leaf = get_leaf_folder(fp)
            p = leaf.replace('\\','/')
            put_sql = f'PUT file://{p}/* @{stage_name}/{ticker} AUTO_COMPRESS=FALSE'
            cur.execute(put_sql)
    finally: 
        cur.close()
        conn.close()
        print('done')