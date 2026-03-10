import sqlite3
import pandas as pd
import logging
import time
from injestiondb import ingest_db
import os
os.makedirs("data/Logs", exist_ok=True)

logging.basicConfig(
    filename='data/Logs/Final_Summary.log',
    level=logging.DEBUG,
    format='%(asctime)s-%(levelname)s-%(message)s',
    filemode='a'

)
def create_finalsummary(conn):
    #To combine all the tables to make one final table
    final_summary = pd.read_sql("""
    WITH 
    freightsummary AS (
        SELECT 
            VendorNumber,
            SUM(Freight) AS totalFreight
        FROM 
            vendor_invoice
        GROUP BY 
            VendorNumber
    ),
    Purchase_Summary AS (
        SELECT 
            pp.VendorNumber,
            pp.VendorName,
            pp.Description,
            pp.Brand,
            pp.PurchasePrice,
            ppp.Price AS Actual_Price,
            ppp.Volume,
            SUM(pp.Quantity) AS TotalPurchaseQuantity,
            SUM(pp.Dollars) AS TotalPurchaseDollars
        FROM 
            purchases pp
            JOIN purchase_prices ppp 
                ON pp.Brand = ppp.Brand
        WHERE 
            pp.PurchasePrice > 0
        GROUP BY 
            pp.VendorNumber,
            pp.VendorName,
            pp.Description,
            pp.Brand,
            pp.PurchasePrice,
            ppp.Price,
            ppp.Volume
    ),
    Sales_summary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM 
            sales
        GROUP BY 
            VendorNo, Brand
    )
    SELECT 
    ps.VendorNumber,
    ps.VendorName,
    ps.Description,
    ps.Brand,
    ps.PurchasePrice,
    ps.Actual_Price,
    ps.Volume,
    ps.TotalPurchaseQuantity,
    ss.TotalSalesDollars,
    ps.TotalPurchaseDollars,
    ss.TotalSalesQuantity,
    ss.TotalSalesPrice,
    ss.TotalExciseTax,
    fs.totalFreight
    FROM 
    Purchase_Summary ps
    LEFT JOIN Sales_summary ss 
        ON ps.VendorNumber = ss.VendorNo 
        AND ps.Brand = ss.Brand
    LEFT JOIN freightsummary fs 
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY 
    ps.TotalPurchaseDollars DESC
    """, conn)
    return final_summary
def clean_data(df):
    df['Volume']=df['Volume'].astype('float64')
    df.fillna(0,inplace=True)
    df['VendorName']=df['VendorName'].str.strip()

    df['Gross_Profit']=df['TotalSalesDollars']-df['TotalPurchaseDollars']
    df['ProfitMargin']=(df['Gross_Profit']/df['TotalSalesDollars'])*100
    df['StockTurnOver']=df['TotalSalesQuantity']/df['TotalPurchaseQuantity']
    df['SaletoPurchaseRatio']=df['TotalSalesDollars']/df['TotalPurchaseDollars']

    return df

if __name__=='__main__':
    start=time.time()
    conn = sqlite3.connect(r"C:\Users\cv\Desktop\DA Projects\project 3\data\inventory.db")
    logging.info('Creatinf the final summary table')
    summary_df=create_finalsummary(conn)
    logging.info(summary_df.head())

    logging.info('Cleaning data')
    clean_df=clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting table')
    ingest_db(clean_df,'final_summary',conn)
    logging.info('Completed')
    end=time.time()
    logging.info(f"Pipeline completed in {end-start:.2f} seconds")



                                                      





