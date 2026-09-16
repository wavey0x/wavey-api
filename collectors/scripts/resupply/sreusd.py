from brownie import chain, interface, Contract, multicall
from config import DAY, UTILITIES, SREUSD
from utils.utils import (
    closest_block_before_timestamp,
    contract_creation_block,
    get_token_logo_url
)

# sreUSD/crvUSD CurveLend market address
SREUSD_MARKET = '0xC32B0Cf36e06c790A568667A17DE80cba95A5Aad'

def get_sreusd_market_data():
    """Collect underlying CurveLend market data for sreUSD/crvUSD market"""
    market = Contract(SREUSD_MARKET)

    # Batch all contract reads with multicall for performance
    with multicall():
        # Get token information
        collat_token_address = market.collateral_token()
        deposit_token_address = market.asset()
        controller_address = market.controller()

        collat_token = Contract(collat_token_address)
        collateral_token_decimals = collat_token.decimals()
        collateral_token_symbol = collat_token.symbol()

        deposit_token = Contract(deposit_token_address)
        deposit_token_symbol = deposit_token.symbol()

        # Get controller and metrics
        controller = Contract(controller_address)
        total_debt_raw = controller.total_debt()
        liquidity_raw = deposit_token.balanceOf(controller_address)
        total_supplied_raw = market.totalAssets()

        # Get AMM and interest rates
        amm_address = controller.amm()
        amm = Contract(amm_address)
        lend_rate_raw = market.lend_apr()
        borrow_rate_raw = amm.rate()

        # Get data for LTV calculation
        collat_balance_raw = collat_token.balanceOf(amm_address)
        oracle_price_raw = amm.price_oracle()

    # Convert to float and perform calculations outside multicall
    total_debt = total_debt_raw / 1e18
    liquidity = liquidity_raw / 1e18
    total_supplied = total_supplied_raw / 1e18
    lend_rate = lend_rate_raw / 1e18
    borrow_rate = borrow_rate_raw * 365 * 86400 / 1e18

    utilization = 0
    if total_supplied > 0:
        utilization = total_debt / total_supplied

    # Calculate global LTV
    collat_value = collat_balance_raw / 10 ** collateral_token_decimals * oracle_price_raw / 1e18
    debt_value = total_debt
    global_ltv = 0
    if collat_value > 0:
        global_ltv = debt_value / collat_value

    # Get token logos (cached, so no need for multicall)
    deposit_token_logo = get_token_logo_url(deposit_token_address)
    collateral_token_logo = get_token_logo_url(collat_token_address)

    return {
        'market_name': 'CurveLend',
        'market': SREUSD_MARKET,
        'collat_token': collat_token_address,
        'deposit_token': deposit_token_address,
        'deposit_token_symbol': deposit_token_symbol,
        'collateral_token_symbol': collateral_token_symbol,
        'collateral_token_decimals': collateral_token_decimals,
        'deposit_token_logo': deposit_token_logo,
        'collateral_token_logo': collateral_token_logo,
        'controller': controller_address,
        'interest_rate_contract': amm_address,
        'total_debt': total_debt,
        'total_supplied': total_supplied,
        'liquidity': liquidity,
        'utilization': utilization,
        'lend_rate': lend_rate,
        'borrow_rate': borrow_rate,
        'global_ltv': global_ltv,
    }

def get_sreusd_data():
    """Collect sreUSD data including market snapshot and historical rates/TVL"""
    utils = interface.IUtilities(UTILITIES)
    sreusd = Contract(SREUSD)
    current_time = int(chain.time())
    deploy_block = contract_creation_block(SREUSD)
    data_points = []

    # Collect all blocks to query first
    blocks_to_query = []
    for day_offset in range(30, 0, -1):
        for hour_offset in [0, 12]:
            timestamp = (current_time - (day_offset * DAY)) // DAY * DAY + hour_offset * 3600
            block = closest_block_before_timestamp(timestamp)
            if block >= deploy_block:
                blocks_to_query.append(block)

    # Query all historical data with multicall per block
    for block in blocks_to_query:
        with multicall(block_identifier=block):
            rate = utils.sreusdRates()
            total_assets_raw = sreusd.totalAssets()
            block_timestamp = chain[block].timestamp

        data_points.append({
            'block': block,
            'timestamp': block_timestamp,
            'rate': rate,
            'apr': rate * 365 * 86400 / 1e18,
            'total_assets': total_assets_raw / 1e18
        })

    # Most recent data point with multicall
    with multicall():
        rate = utils.sreusdRates()
        total_assets_raw = sreusd.totalAssets()

    data_points.append({
        'block': chain.height,
        'timestamp': current_time,
        'rate': rate,
        'apr': rate * 365 * 86400 / 1e18,
        'total_assets': total_assets_raw / 1e18
    })

    # Return nested structure with market data and historical data
    return {
        'market_data': get_sreusd_market_data(),
        'historical_data': data_points
    }
