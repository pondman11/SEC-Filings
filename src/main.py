from scripts.load_files import upload_files, get_config

if __name__ == "__main__":
    upload_files("sec-edgar-filings", f'{get_config("database")}.{get_config("schema")}.{get_config("stage")}')
    print("Download process completed.")