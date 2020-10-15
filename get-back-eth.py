from web3 import Web3, HTTPProvider
from time import sleep
from requests import RequestException, ConnectionError, Timeout
import requests
import csv
import configparser

c = configparser.ConfigParser()
c.read("config.ini")
c = c["DEFAULT"]

file_log_name          = str(c["file_log_name"])
csv_delimiter          = str(c["csv_delimiter"])
csv_file_encoding      = str(c["csv_file_encoding"])
file_with_keys         = str(c["file_with_keys"])
infura_provider        = str(c["infura_provider"])
recipient_address      = str(c["recipient_address"])
eth_transfer_gas_limit = int(c["eth_transfer_gas_limit"])
gas_price_level        = str(c["gas_price_level"])
w3 = Web3(HTTPProvider(infura_provider))


def write_log(log_string: str):
    with open(file_log_name, 'a') as logFile:
        logFile.write(log_string + '\n')


def csv_reader(csv_file: str = file_with_keys) -> list:
    with open(csv_file, 'r', encoding=csv_file_encoding) as f:
        reader = csv.reader(f, delimiter=csv_delimiter)
        addr_prv_list = []
        for line_number, row in enumerate(reader, start=1):
            addr = row[0]
            prv = row[1]
            if type(addr) is not str:
                break

            if addr[0:2] == '0x' and len(addr) == 42 and len(prv) == 64:
                addr_prv_list.append(f'{addr};{prv}')

            elif line_number > 3 and addr[0:2] != '0x' and len(addr) != 42:
                break

            else:
                continue
        return addr_prv_list


def get_gas_price():
    try:
        req = requests.get("https://ethgasstation.info/api/ethgasAPI.json")
        if req.status_code == 200 and "safeLow" in str(req.content):
            gwei_price = int(int(req.json()[gas_price_level]) / 10 * 1e9)
            print(f'Gas price level {gas_price_level}: {gwei_price / 1e9} gwei')
            return gwei_price

    except (Exception, RequestException, ConnectionError, Timeout) as gas_price_err:
        print("Can't get current gas price --> exit")
        print(gas_price_err)
        exit()


def get_actual_nonce(address: str) -> int:
    return w3.eth.getTransactionCount(Web3.toChecksumAddress(address))


def get_eth_balance(address: str) -> int:
    return w3.eth.getBalance(Web3.toChecksumAddress(address))


def get_eth_signed_tx(sender_nonce: int, private_key: str, amount: int, gas_price: int) -> str:
    eth_signed_tx = w3.eth.account.signTransaction(dict(
        nonce=sender_nonce,
        gasPrice=gas_price,
        gas=eth_transfer_gas_limit,
        to=Web3.toChecksumAddress(recipient_address),
        value=amount,
        data=b'',
      ),
      private_key
    )
    return eth_signed_tx


keypairs = csv_reader(file_with_keys)
print(f'Load {len(keypairs)} keypairs')
current_gas_price = int(get_gas_price())

for i, keypair in enumerate(keypairs, start=1):
    try:
        addr = keypair.split(";")[0]
        priv = keypair.split(";")[1]

        address_balance = get_eth_balance(addr)
        amount_to_send = address_balance - (eth_transfer_gas_limit * current_gas_price)
        if amount_to_send > 1:
            print(f'{i}\\{len(keypairs)} | Sending from {addr} to {recipient_address} {amount_to_send / 1e18} ETH')
            current_nonce = get_actual_nonce(addr)
            eth_signed_tx = get_eth_signed_tx(sender_nonce=current_nonce, private_key=priv,
                                              amount=amount_to_send, gas_price=current_gas_price)
            tx_id_bin = w3.eth.sendRawTransaction(eth_signed_tx.rawTransaction)
            tx_id_hex = Web3.toHex(tx_id_bin)
            tx_id = f'TX_ID: https://etherscan.io/tx/{tx_id_hex}'
            print(tx_id)
            log_string = f'{str(i)} | {tx_id} | {str(eth_signed_tx)}'
            write_log(log_string)
            sleep(0.15)

        else:
            print(f'{i}\\{len(keypairs)} | https://etherscan.io/address/{addr}'
                  f' | Balance {address_balance / 1e18} ETH'
                  f' | Insufficient funds {amount_to_send} --> SKIP')

    except Exception as unknwErr:
        print(unknwErr)
        sleep(0.25)

print("Done")
