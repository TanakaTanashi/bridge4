from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from pathlib import Path
import json
import pandas as pd

def scan_blocks(chain, start_block, end_block, contract_address,
                eventfile: str = "deposit_logs.csv"):

    if chain == "avax":
        api_url = "https://api.avax-test.network/ext/bc/C/rpc"
    elif chain == "bsc":
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"
    else:
        raise ValueError("chain must be either 'avax' or 'bsc'")

    w3 = Web3(Web3.HTTPProvider(api_url))
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)


    DEPOSIT_ABI = json.loads("""
    [
      {
        "anonymous": false,
        "inputs": [
          {"indexed": true,  "internalType": "address", "name": "token",     "type": "address"},
          {"indexed": true,  "internalType": "address", "name": "recipient", "type": "address"},
          {"indexed": false, "internalType": "uint256", "name": "amount",    "type": "uint256"}
        ],
        "name": "Deposit",
        "type": "event"
      }
    ]""")

    contract = w3.eth.contract(address=contract_address, abi=DEPOSIT_ABI)


    if start_block == "latest":
        start_block = w3.eth.block_number
    if end_block == "latest":
        end_block = w3.eth.block_number

    if end_block < start_block:
        raise ValueError("end_block must be >= start_block")


    def _write_events(events):
        if not events:
            return
        rows = [{
            "chain":           chain,
            "token":           evt.args["token"],
            "recipient":       evt.args["recipient"],
            "amount":          int(evt.args["amount"]),
            "transactionHash": evt.transactionHash.hex(),
            "address":         evt.address
        } for evt in events]

        df = pd.DataFrame(rows)
        header = not Path(eventfile).exists()
        df.to_csv(eventfile, mode="a", header=header, index=False)

    # Query
    if end_block - start_block < 30:

        events = contract.events.Deposit.create_filter(
            from_block=start_block,
            to_block=end_block,
            argument_filters={}
        ).get_all_entries()
        _write_events(events)
    else:

        for blk in range(start_block, end_block + 1):
            events = contract.events.Deposit.create_filter(
                from_block=blk,
                to_block=blk,
                argument_filters={}
            ).get_all_entries()
            _write_events(events)
