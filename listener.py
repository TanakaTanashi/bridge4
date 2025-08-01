from web3 import Web3
from web3.providers.rpc import HTTPProvider
from web3.middleware import ExtraDataToPOAMiddleware
from pathlib import Path
import json
from datetime import datetime
import pandas as pd


def scan_blocks(chain, start_block, end_block, contract_address, eventfile='deposit_logs.csv'):
    """
    chain - string (Either 'bsc' or 'avax')
    start_block - integer first block to scan
    end_block - integer last block to scan
    contract_address - the address of the deployed contract

    This function reads "Deposit" events from the specified contract,
    and writes information about the events to the file "deposit_logs.csv"
    """
    if chain == 'avax':
        api_url = "https://api.avax-test.network/ext/bc/C/rpc"
    elif chain == 'bsc':
        api_url = "https://data-seed-prebsc-1-s1.binance.org:8545/"
    else:
        raise ValueError(f"Unsupported chain: {chain}")

    w3 = Web3(Web3.HTTPProvider(api_url))
    if chain in ['avax', 'bsc']:
        w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)

    DEPOSIT_ABI = json.loads(
        '[ { "anonymous": false, "inputs": [ '
        '{ "indexed": true, "internalType": "address", "name": "token", "type": "address" },'
        ' { "indexed": true, "internalType": "address", "name": "recipient", "type": "address" },'
        ' { "indexed": false, "internalType": "uint256", "name": "amount", "type": "uint256" } ],'
        ' "name": "Deposit", "type": "event" }]' }
    )
    contract = w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=DEPOSIT_ABI)

    if start_block == 'latest':
        start_block = w3.eth.get_block_number()
    if end_block == 'latest':
        end_block = w3.eth.get_block_number()

    if end_block < start_block:
        raise ValueError(f"end_block ({end_block}) < start_block ({start_block})")

    if start_block == end_block:
        print(f"Scanning block {start_block} on {chain}")
    else:
        print(f"Scanning blocks {start_block} - {end_block} on {chain}")

    rows = []
    def process_events(events):
        for evt in events:
            try:
                blk = w3.eth.get_block(evt.blockNumber)
                ts = datetime.fromtimestamp(blk.timestamp).strftime("%m/%d/%Y %H:%M:%S")
            except Exception:
                ts = datetime.now().strftime("%m/%d/%Y %H:%M:%S")
            rows.append({
                'chain': chain,
                'token': evt.args['token'],
                'recipient': evt.args['recipient'],
                'amount': evt.args['amount'],
                'transactionHash': evt.transactionHash.hex(),
                'address': evt.address,
                'date': ts
            })

      if end_block - start_block < 30:
        event_filter = contract.events.Deposit.createFilter(
            fromBlock=start_block, toBlock=end_block, argument_filters={}
        )
        events = event_filter.get_all_entries()
        process_events(events)
    else:
        for blk_num in range(start_block, end_block + 1):
            event_filter = contract.events.Deposit.createFilter(
                fromBlock=blk_num, toBlock=blk_num, argument_filters={}
            )
            events = event_filter.get_all_entries()
            process_events(events)

    df = pd.DataFrame(rows)
    out_path = Path(eventfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {len(rows)} event(s) to {eventfile}")

