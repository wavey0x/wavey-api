"""Finalized boundaries shared by the retained feed collectors and importer."""

from utils.feeds import CollectionError


def block_hash(block):
    value = block['hash']
    return (value if isinstance(value, str) else value.hex()).lower().removeprefix('0x')


def finalized_boundary(w3, state):
    if w3.eth.chain_id != 1:
        raise CollectionError('wrong_chain')
    block = w3.eth.get_block('finalized')
    for cursor in state['cursors'].values():
        if cursor['block'] > block['number']:
            raise CollectionError('checkpoint_not_finalized')
        verify_block(w3, cursor['block'], cursor['hash'])
    return block['number'], block_hash(block)


def verify_block(w3, height, expected_hash):
    if block_hash(w3.eth.get_block(height)) != expected_hash:
        raise CollectionError('checkpoint_changed')


def advance(state, height, expected_hash, w3):
    # Recheck old boundaries as well as the collected end before committing.
    for cursor in state['cursors'].values():
        verify_block(w3, cursor['block'], cursor['hash'])
    verify_block(w3, height, expected_hash)
    state['cursors'] = {name: {'block': height, 'hash': expected_hash} for name in state['cursors']}
    state.pop('pending', None)
    return state


def event_logs(event, start, end, chunk_size=10_000):
    for first in range(start, end + 1, chunk_size):
        last = min(first + chunk_size - 1, end)
        seen = set()
        logs = event.get_logs(fromBlock=first, toBlock=last)
        for log in sorted(logs, key=lambda row: (row['blockNumber'], row['logIndex'])):
            if not first <= log['blockNumber'] <= last:
                raise CollectionError('event_outside_requested_range')
            key = (str(log['transactionHash']), log['logIndex'])
            if key not in seen:
                seen.add(key)
                yield log
