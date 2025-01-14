import requests
from bs4 import BeautifulSoup

import time

import requests
import time
import pandas as pd
import polars as pl

global tracker
tracker = []


def equity_figi_data(exchCode=None, securityType=None, includeUnlistedEquities=False):
    url = "https://api.openfigi.com/v3/search"
    headers = {
        "Content-Type": "application/json",
        "X-OPENFIGI-APIKEY": "27240821-9956-4377-a5d6-895bc51059d4"
    }

    marketSecDes = 'Equity'

    payload = {
        "exchCode": exchCode,
        "marketSecDes": marketSecDes,
        "securityType": securityType,
        "includeUnlistedEquities": includeUnlistedEquities,
    }

    results = []
    columns = {
                'figi': ['00'], 
                'name': ['00'], 
                'ticker': ['00'], 
                'exchCode': ['00'], 
                'compositeFIGI': ['00'], 
                'securityType': ['00'], 
                'marketSector': ['00'], 
                'shareClassFIGI': ['00'], 
                'securityType2': ['00'], 
                'securityDescription': ['00']
                }

    while True:
        response = requests.post(url, json=payload, headers=headers)
        print(f"Status Code: {response.status_code}")
        
        try:
            response_data = response.json()

        except requests.exceptions.JSONDecodeError:
            print("Error decoding JSON response.")
            break
        
        if 'data' in response_data:
            results.extend(response_data['data'])
        
        next_page = response_data.get('next')
        if not next_page:
            time.sleep(3)  # Rate limiting
            break
        
        payload["start"] = next_page  
        time.sleep(3)  # Rate limiting

    if results:
        tracker.append(securityType)
        return pl.DataFrame(results)
    else:
        return pl.DataFrame(columns, schema={
            'figi': pl.Utf8, 
            'name': pl.Utf8, 
            'ticker': pl.Utf8, 
            'exchCode': pl.Utf8, 
            'compositeFIGI': pl.Utf8, 
            'securityType': pl.Utf8, 
            'marketSector': pl.Utf8, 
            'shareClassFIGI': pl.Utf8, 
            'securityType2': pl.Utf8, 
            'securityDescription': pl.Utf8
        })
    
def equity_figi_data(exchCode=None, securityType=None, includeUnlistedEquities=False):
    url = "https://api.openfigi.com/v3/search"
    headers = {
        "Content-Type": "application/json",
        "X-OPENFIGI-APIKEY": "27240821-9956-4377-a5d6-895bc51059d4"
    }

    marketSecDes = 'Equity'

    payload = {
        "exchCode": exchCode,
        "marketSecDes": marketSecDes,
        "securityType": securityType,
        "includeUnlistedEquities": includeUnlistedEquities,
    }

    results = []
    columns = {
                'figi': ['00'], 
                'name': ['00'], 
                'ticker': ['00'], 
                'exchCode': ['00'], 
                'compositeFIGI': ['00'], 
                'securityType': ['00'], 
                'marketSector': ['00'], 
                'shareClassFIGI': ['00'], 
                'securityType2': ['00'], 
                'securityDescription': ['00']
                }

    while True:
        response = requests.post(url, json=payload, headers=headers)
        print(f"Status Code: {response.status_code}")
        
        try:
            response_data = response.json()

        except requests.exceptions.JSONDecodeError:
            print("Error decoding JSON response.")
            break
        
        if 'data' in response_data:
            results.extend(response_data['data'])
        
        next_page = response_data.get('next')
        if not next_page:
            time.sleep(3)  # Rate limiting
            break
        
        payload["start"] = next_page  
        time.sleep(3)  # Rate limiting

    if results:
        tracker.append(securityType)
        return pl.DataFrame(results)
    else:
        return pl.DataFrame(columns, schema={
            'figi': pl.Utf8, 
            'name': pl.Utf8, 
            'ticker': pl.Utf8, 
            'exchCode': pl.Utf8, 
            'compositeFIGI': pl.Utf8, 
            'securityType': pl.Utf8, 
            'marketSector': pl.Utf8, 
            'shareClassFIGI': pl.Utf8, 
            'securityType2': pl.Utf8, 
            'securityDescription': pl.Utf8
        })

def equity_db():
    url = 'https://api.openfigi.com/v3/mapping/values/exchCode'
    response = requests.get(url)
    exch_codes = response.json()
    exch_codes = exch_codes['values']
    remove = ['AC', 'AE', 'AF', 'AM', 'AS', 'AH', 'AN', 'AO', 'AQ', 'AT', 'PF', 'SI', 'BE', 'BL', 'BN', 'BO', 
              'BR', 'BS', 'BV', 'CL', 'CO', 'CX', 'CG', 'CS', 'JC', 'QN', 'CC', 'CE', 'CA', 'CF', 'CJ', 'CM', 
              'CQ', 'CT', 'CV', 'CW', 'DG', 'DJ', 'DK', 'DL', 'DM', 'DN', 'DS', 'DT', 'DV', 'QF', 'QG', 'QH', 
              'TA', 'TG', 'TJ', 'TK', 'TN', 'TO', 'TR', 'TV', 'TW', 'TX', 'TY', 'HX', 'HD', 'CD', 'CK', 'KL', 
              'RC', 'VA', 'ZA', 'EG', 'EQ', 'EA', 'EC', 'EI', 'GB', 'GC', 'GD', 'GE', 'GF', 'GH', 'GI', 'GM', 
              'GQ', 'GS', 'GT', 'GW', 'GY', 'LA', 'LU', 'IB', 'IG', 'IH', 'IS', 'JB', 'JD', 'JE', 'JF', 'JG', 
              'JI', 'JJ', 'JK', 'JM', 'JN', 'JO', 'JQ', 'JS', 'JT', 'JU', 'JV', 'JW', 'JX', 'KE', 'KP', 'KQ', 
              'LF', 'LG', 'LV', 'MF', 'MU', 'PK', 'RN', 'RP', 'RX', 'RE', 'RQ', 'RZ', 'RG', 'RR', 'RS', 'RT', 
              'SA', 'SB', 'SN', 'SO', 'SQ', 'ST', 'BW', 'SC', 'SE', 'SR', 'SX', 'XK', 'DB', 'DH', 'PQ', 'UA', 
              'UB', 'UC', 'UD', 'UE', 'UF', 'UI', 'UJ', 'UL', 'UM', 'UN', 'UO', 'UP', 'UQ', 'UR', 'UT', 'UU', 
              'UV', 'UW', 'UX', 'VF', 'VG', 'VJ', 'VK', 'VL', 'VP', 'VY', 'VE', 'VS', 'VH', 'VM', 'VU', 'B1', 
              'E1', 'X1', 'X2', 'XA', 'XB', 'XC', 'XD', 'XE', 'XF', 'XG', 'XH', 'XI', 'XJ', 'XL', 'XM', 'XN', 
              'XO', 'XP', 'XQ', 'XR', 'XS', 'XT', 'XU', 'XV', 'XW', 'XX', 'XY', 'XZ', 'Z1', 'AL', 'AI', 'AW', 
              'A0', 'C3', 'NB', 'ZB', 'BP', 'BX', 'E2', 'ZC', 'HE', 'CU', 'YC', 'DD', 'DF', 'DX', 'EM', 'EN', 
              'EE', 'EF', 'FF', 'NP', 'ZG', 'GX', 'NF', 'NM', 'NY', 'AA', 'AP', 'GL', 'G4', 'HO', 'RF', 'IK', 
              'IY', 'I9', 'IC', 'IF', 'NI', 'IA', 'AK', 'KX', 'KO', 'LY', 'LO', 'MD', 'MI', 'MQ', 'ES', 'NE', 
              'NN', 'NQ', 'NX', 'NV', 'EX', 'NC', 'NJ', 'NS', 'LD', 'OF', 'PX', 'PC', 'PD', 'PT', 'P2', 'AJ', 
              'KA', 'NG', 'SF', 'TD', 'MT', 'Z2', 'T0', 'TF', 'TS', 'TM', 'TC', 'OU', 'QU', 'UK', 'LC', 'LT', 
              'YS', '~', 'Crypto Exchanges', 'Exchange Code', 'abts', 'acxi', 'alcn', 'bbit', 'bbox', 'bbsp', 
              'bcex', 'bequ', 'bfly', 'bfnx', 'bfrx', 'bgon', 'binc', 'bitc', 'bitz', 'bjex', 'bl3p', 'blc2', 
              'blcr', 'bnbd', 'bnce', 'bndx', 'bnf8', 'bnus', 'bopt', 'bpnd', 'bt38', 'btba', 'btbu', 'btby', 
              'btca', 'btcb', 'btcc', 'bthb', 'btma', 'btmx', 'btrk', 'btrx', 'btsh', 'btso', 'bull', 'bxth', 
              'bybt', 'cbse', 'ccck', 'ccex', 'cexi', 'cflr', 'cflx', 'cnex', 'cngg', 'cnhd', 'cnmt', 'cone', 
              'crco', 'crfl', 'crtw', 'crv2', 'cucy', 'curv', 'delt', 'drbt', 'dydx', 'eris', 'ethx', 'etrx', 
              'exxa', 'ftxu', 'ftxx', 'gacn', 'gate', 'gmni', 'hbdm', 'hitb', 'huob', 'inch', 'indr', 'itbi', 
              'kcon', 'korb', 'krkn', 'lclb', 'lgom', 'lmax', 'merc', 'mexc', 'mtgx', 'ngcs', 'nova', 'nvdx', 
              'okcn', 'okex', 'oslx', 'pksp', 'polo', 'qsp2', 'qsp3', 'quon', 'sghd', 'stmp', 'sush', 'sxha', 
              'tbit', 'tidx', 'tj21', 'tjv1', 'tjv2', 'tocn', 'trck', 'uexx', 'upbt', 'usp2', 'usp3', 'vtro', 
              'wexn', 'xcme', 'yobt', 'zaif', 'zbcn']

    exch_codes = [value for value in exch_codes if value not in remove]

    securities = [
        'Bond', 'Closed-End Fund', 'Common Stock', 'Conv Bond', 
        'Conv Prfd', 'Fund of Funds', 'Mutual Fund', 'Open-End Fund', 
        'Preferred', 'Pvt Eqty Fund', 'REIT'
    ]  ## WARRANT's MAY EXIST BUT NO DATA FOR CURRENT VERSION

    df = pl.DataFrame()

    for exch_code in exch_codes:
        for security in securities:
            print(exch_code, security)
            data = equity_figi_data(exchCode=exch_code, securityType=security, includeUnlistedEquities=False)
            print(data)

            df = pl.concat([df, data], how="vertical")
            time.sleep(3)  # Rate limiting

    new_headers = ['FIGI', 'NAME', 'TICKER', 'EXCHANGE_CODE', 'COMP_FIGI', 'TYPE', 'MARKET_TYPE', 'SHARE_CLASS_FIGI', 'SECURITY_TYPE', 'DESCRIPTION']
    df = df.rename({old: new for old, new in zip(df.columns, new_headers)})
    
    df = df.with_columns(
        pl.col("NAME").str.to_uppercase().alias("NAME")
        )
    df = df.with_columns(
        pl.col("TYPE").str.to_uppercase().alias("TYPE")
        )  
    df = df.with_columns(
        pl.col("MARKET_TYPE").str.to_uppercase().alias("MARKET_TYPE")
        )  
    df = df.with_columns(
        pl.col("SECURITY_TYPE").str.to_uppercase().alias("SECURITY_TYPE")
        )  

    df = df.select(['FIGI', 'TICKER', 'NAME', 'EXCHANGE_CODE', 'TYPE', 'MARKET_TYPE', 'SECURITY_TYPE'])

    return df

def exchange_db():
    file = 'https://www.openfigi.com/assets/content/OpenFIGI_Exchange_Codes-3d3e5936ba.csv'

    df = pl.read_csv(file)
    df = df.slice(1)

    new_headers = ["EXCHANGE_CODE", "OPENFIGI_EXCHANGE", "COUNTRY_CODE", "COUNTRY", "ISO", "TRUE", "EXCHANGE"]
    df = df.rename({old: new for old, new in zip(df.columns, new_headers)})
    df = df.slice(1)

    df = df.select(['EXCHANGE_CODE', 'EXCHANGE', 'COUNTRY_CODE', 'COUNTRY'])
    df = df.drop_nulls()

    df = df.with_columns(
        pl.col("EXCHANGE").str.to_uppercase().alias("EXCHANGE")
        )  
    df = df.with_columns(
        pl.col("COUNTRY").str.to_uppercase().alias("COUNTRY")
        )   

    return df

def bw_db():
    df2 = exchange_db()
    df1 = equity_db()

    df = df1.join(df2, on='EXCHANGE_CODE', how='left')

    df_p = df.to_pandas()
    df_p = df_p[df_p['FIGI'] != '00']
    df_p.to_feather(r"C:\Users\YOUR_NAME\YOUR_LOCATION.feather")
    print(df)

    unique_values = list(set(tracker))
    print(unique_values)

    return df


bw_db()
