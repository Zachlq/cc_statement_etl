import PyPDF2
import pandas as pd
import os
from google.cloud import bigquery
import logging
import warnings
import datetime
import config as cfg
from google.cloud import storage

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO, datefmt='%I:%M:%S')

bq_client = bigquery.Client()
storage_client = storage.Client()

today = datetime.datetime.now()

prev = today - timedelta(days=30)
prev_str = prev.strftime("%Y-%m-%d")

today_str = today.strftime("%Y-%m-%d")

def get_statement(bucket_name: str, prefix: str)->None:

  bucket = storage_client.get_bucket(bucket_name)
  blob = bucket.list_blobs(prefix=prefix)

  for b in blob:
    blob = bucket.blob(b)
    blob.download_to_filename(b)

def upload_to_bq(df: pd.DataFrame, dataset_id: str, table_id: str, schema: list)->None:

    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition='WRITE_APPEND'
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.schema = schema
    job_config.ignore_unknown_values=True 

    job = bq_client.load_table_from_dataframe(
    df,
    table_ref,
    location='US',
    job_config=job_config)
    
    return job.result()

def parse_statement(statement: str):

    reader = PyPDF2.PdfReader(statement)
    end_page = len(reader.pages)
    
    for p in range(0,end_page):

        first_page = reader.pages[p].extract_text()
        
        spl = first_page.split("\n")

        items = spl[0:len(spl)]
        
        df = pd.DataFrame(items)
        
        df["total"] = df[df[0].str.contains("Purchases") == True]
        df.drop(0, axis=1, inplace=True)
        df.dropna(inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        for r in cfg.repl_lst:
            df["total"] = df["total"].str.replace(r, "")
            
        df["total"] = df["total"].astype(float)
            
        return df

def create_dataframe(df: pd.DataFrame, period_start: str, period_end: str, cols: list)->None:

  df["period_start"] = period_start
  df["period_end"] = period_end

  df["dt_updated"] = pd.Timestamp.utcnow()

  df = df[cols]

  return df

def chase_sapphire(event, context)->None:

    logging.info("Downloading statement...")
  
    statement = get_statement(cfg.bucket_name, cfg.prefix)
  
    logging.info("Parsing statement...")
    
    account_statement = parse_statement(latest_statement)
    
    logging.info("Creating data frame...")
    
    final_df = create_dataframe(account_statement, prev_str, today_str)
    
    logging.info(f"Uploading to {cfg.dataset}.{cfg.sapphire_table}...")
    
    upload_to_bq(final_df, cfg.dataset, cfg.sapphire_table, cfg.sapphire_schema)
    
    logging.info(f"{cfg.dataset}.{cfg.sapphire_table} updated for {today_str}.")

if __name__ == "__main__":
    chase_sapphire("","")
