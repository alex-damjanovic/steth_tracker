import pandas as pd
from web3 import Web3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import json
from decimal import Decimal


# make sure to put everything in .env
def load_environment_variables():
    load_dotenv()
    alchemy_api_key = os.getenv('ALCHEMY_API_KEY')
    user_address = os.getenv('USER_ADDRESS')
    contract_address = os.getenv('STETH_ADDRESS')
    if not alchemy_api_key:
        raise ValueError("Alchemy API key not found in the environment variables!!")
    if not user_address:
        raise ValueError("User address not found in the environment variables!!")
    if not contract_address:
        raise ValueError("Contract address not found in the environment variables!!")
    return alchemy_api_key, user_address, contract_address

# connecting to stuff
def initialize_web3(alchemy_api_key):
    alchemy_url = f'https://eth-mainnet.alchemyapi.io/v2/{alchemy_api_key}'
    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    if not w3.is_connected():
        raise ConnectionError("Failed to connect to the Ethereum network")
    else:
        print("Connected to Ethereum!")
    return w3

# we need abi so we can call contract methods
def load_contract_abi(filepath='abi.json'):
    with open(filepath, 'r') as abi_file:
        contract_abi = json.load(abi_file)
    return contract_abi

# creating instance
def get_contract_instance(w3, contract_address, contract_abi):
    return w3.eth.contract(address=contract_address, abi=contract_abi)

# querying current info
def query_contract_data(contract, query_address):
    shares = contract.functions.sharesOf(query_address).call()
    total_pooled_ether = contract.functions.getTotalPooledEther().call()
    total_shares = contract.functions.getTotalShares().call()
    balance = contract.functions.balanceOf(query_address).call()
    return shares, total_pooled_ether, total_shares, balance

# getting block timestamp
def get_current_block_timestamp(w3):
    latest_block = w3.eth.get_block('latest')
    block_timestamp = latest_block['timestamp']
    return datetime.fromtimestamp(block_timestamp, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S')

# loading previous data so we can calculate diffs in balance, shares, ether etc
def load_previous_data(filepath='results.csv'):
    if os.path.exists(filepath):
        df = pd.read_csv(filepath)
    else:
        df = pd.DataFrame(columns=[
            'BlockTime', 'Address', 'Balance', 'ChangeInBalance', 'Shares', 'ChangeInShares', 'TotalShares', 'ChangeInTotalShares', 
            'TotalPooledEther', 'ChangeInTotalPooledEther'
        ])
    return df

# frankenstein function but does the job :) 
def calculate_differences(df, query_address, shares_decimal, balance_decimal, total_shares_decimal, total_ether_decimal):
    if not df.empty and query_address in df['Address'].values:
        last_shares = Decimal(df.loc[df['Address'] == query_address, 'Shares'].values[-1])
        last_balance = Decimal(df.loc[df['Address'] == query_address, 'Balance'].values[-1])
        last_total_shares = Decimal(df.loc[df['Address'] == query_address, 'TotalShares'].values[-1])
        last_total_ether = Decimal(df.loc[df['Address'] == query_address, 'TotalPooledEther'].values[-1])
        difference_shares = shares_decimal - last_shares
        difference_balance = balance_decimal - last_balance
        difference_total_shares = total_shares_decimal - last_total_shares
        difference_total_ether = total_ether_decimal - last_total_ether
    else:
        difference_shares = difference_balance = difference_total_shares = difference_total_ether = Decimal(0)
    return difference_shares, difference_balance, difference_total_shares, difference_total_ether

# saving to csv 
def save_data(df, new_row, filepath='results.csv'):
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(filepath, index=False)
    return df


def main():
    alchemy_api_key, query_address, contract_address = load_environment_variables()
    w3 = initialize_web3(alchemy_api_key)

    contract_abi = load_contract_abi()
    contract = get_contract_instance(w3, contract_address, contract_abi)

    shares, total_pooled_ether, total_shares, balance = query_contract_data(contract, query_address)

    block_time = get_current_block_timestamp(w3)
    df = load_previous_data()

    difference_shares, difference_balance, difference_total_shares, difference_total_ether = calculate_differences(
        df, query_address, Decimal(shares), Decimal(balance), Decimal(total_shares), Decimal(total_pooled_ether))

    # i am transforming it to strings in the csv since it used to cut the values
    new_row = pd.DataFrame([{
        'BlockTime': block_time,
        'Address': query_address,
        'Balance': str(balance),
        'ChangeInBalance': str(difference_balance),
        'Shares': str(shares),
        'ChangeInShares': str(difference_shares),
        'TotalPooledEther': str(total_pooled_ether),
        'ChangeInTotalPooledEther': str(difference_total_ether),
        'TotalShares': str(total_shares),
        'ChangeInTotalShares': str(difference_total_shares)
    }])

    df = save_data(df, new_row)
    print(df)
    print('Check results.csv!!')


if __name__ == "__main__":
    main()
