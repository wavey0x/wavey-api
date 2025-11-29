from database import db

class Stake(db.Model):
    __tablename__ = 'stakes'
    
    id = db.Column(db.Integer, primary_key=True)
    ybs = db.Column(db.String(), nullable=False)
    account = db.Column(db.String(), nullable=False)
    amount = db.Column(db.Numeric, nullable=False)
    newweight = db.Column(db.Numeric, nullable=False)
    timestamp = db.Column(db.Integer, nullable=False)
    staked = db.Column(db.Boolean, nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'ybs': self.ybs,
            'staked': self.staked,
            'account': self.account,
            'amount': float(self.amount),   # Convert Decimal/Numeric here if necessary
            'newweight': float(self.newweight),  # Assuming Numeric needs float conversion
            'timestamp': int(self.timestamp)
        }


class UserWeekInfo(db.Model):
    __tablename__ = 'user_week_info'

    account = db.Column(db.String, primary_key=True)
    week_id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String)
    user_weight = db.Column(db.Numeric(30, 18))
    user_balance = db.Column(db.Numeric(30, 18))
    user_boost = db.Column(db.Numeric(30, 18))
    user_stake_map = db.Column(db.JSON)
    user_rewards_earned = db.Column(db.Numeric(30, 18))
    ybs = db.Column(db.String)
    global_weight = db.Column(db.Numeric(30, 18))
    global_stake_map = db.Column(db.JSON)
    start_ts = db.Column(db.Integer)
    start_block = db.Column(db.Integer)
    end_ts = db.Column(db.Integer)
    end_block = db.Column(db.Integer)
    start_time_str = db.Column(db.String)
    end_time_str = db.Column(db.String)

    def to_dict(self):
        return {
            'account': self.account,
            'week_id': self.week_id,
            'token': self.token,
            'weight': float(self.user_weight),
            'balance': float(self.user_balance),
            'boost': float(self.user_boost),
            'stake_map': self.user_stake_map,
            'rewards_earned': float(self.user_rewards_earned),
            'ybs': self.ybs,
            'global_weight': float(self.global_weight),
            'global_stake_map': self.global_stake_map,
            'start_ts': self.start_ts,
            'start_block': self.start_block,
            'end_ts': self.end_ts,
            'end_block': self.end_block,
            'start_time_str': self.start_time_str,
            'end_time_str': self.end_time_str,
        }
    

class UserInfo(db.Model):
    __tablename__ = 'user_info'
    account = db.Column(db.String, primary_key=True)
    week_id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String)
    weight = db.Column(db.Numeric(30, 18))
    balance = db.Column(db.Numeric(30, 18))
    boost = db.Column(db.Numeric(30, 18))
    stake_map = db.Column(db.JSON)
    rewards_earned = db.Column(db.Numeric(30, 18))
    ybs = db.Column(db.String)
    def to_dict(self):
        return {
            'account': self.account,
            'week_id': self.week_id,
            'token': self.token,
            'weight': float(self.weight),
            'balance': float(self.balance),
            'boost': float(self.boost),
            'stake_map': self.stake_map,
            'rewards_earned': float(self.rewards_earned),
            'ybs': self.ybs
        }


class GlobalWeekInfo(db.Model):
    __tablename__ = 'week_info'
    week_id = db.Column(db.Integer, primary_key=True)
    token = db.Column(db.String)
    weight = db.Column(db.Numeric(30, 18))
    total_supply = db.Column(db.Numeric(30, 18))
    boost = db.Column(db.Numeric(30, 18))
    stake_map = db.Column(db.JSON)
    ybs = db.Column(db.String)
    start_ts = db.Column(db.Integer)
    end_ts = db.Column(db.Integer)
    start_block = db.Column(db.Integer)
    end_block = db.Column(db.Integer)
    start_time_str = db.Column(db.String)
    end_time_str = db.Column(db.String)
    def to_dict(self):
        return {
            'week_id': self.week_id,
            'token': self.token,
            'weight': float(self.weight),
            'total_supply': float(self.total_supply),
            'balance': float(self.total_supply),
            'boost': float(self.boost),
            'stake_map': self.stake_map,
            'ybs': self.ybs,
            'start_ts': self.start_ts,
            'start_block': self.start_block,
            'end_ts': self.end_ts,
            'end_block': self.end_block,
            'start_time_str': self.start_time_str,
            'end_time_str': self.end_time_str,
        }


class CrvLlHarvest(db.Model):
    __tablename__ = 'crv_ll_harvests'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    profit = db.Column(db.Numeric(30, 18))
    timestamp = db.Column(db.Integer)
    name = db.Column(db.String)
    underlying = db.Column(db.String)
    compounder = db.Column(db.String)
    block = db.Column(db.Integer)
    txn_hash = db.Column(db.String)
    date_str = db.Column(db.String)

class GaugeVoteInfo(db.Model):
    __tablename__ = 'curve_gauge_votes'
    
    id = db.Column(db.Integer, primary_key=True)
    gauge = db.Column(db.String, nullable=False)
    gauge_name = db.Column(db.String, nullable=True)
    account = db.Column(db.String, nullable=False)
    amount = db.Column(db.Numeric(30, 18), nullable=False)
    weight = db.Column(db.Integer, nullable=False)
    txn_hash = db.Column(db.String, nullable=True)
    timestamp = db.Column(db.Integer, nullable=False)
    date_str = db.Column(db.String, nullable=True)
    block = db.Column(db.Integer, nullable=False)
    account_alias = db.Column(db.String, nullable=True)

    def to_dict(self):
        return {
            'id': self.id,
            'gauge': self.gauge,
            'gauge_name': self.gauge_name,
            'account': self.account,
            'amount': float(self.amount),
            'weight': self.weight,
            'txn_hash': self.txn_hash,
            'timestamp': self.timestamp,
            'date_str': self.date_str,
            'block': self.block,
            'account_alias': self.account_alias
        }


class RsupIncentive(db.Model):
    __tablename__ = 'rsup_incentives'
    
    id = db.Column(db.Integer, primary_key=True)
    epoch = db.Column(db.Integer, nullable=False)
    total_incentives = db.Column(db.Float, nullable=False)
    convex_amount = db.Column(db.Float, nullable=False)
    yearn_amount = db.Column(db.Float, nullable=False)
    convex_votes_per_usd = db.Column(db.Float, nullable=False)
    yearn_votes_per_usd = db.Column(db.Float, nullable=False)
    convex_votes = db.Column(db.Float, nullable=False)
    yearn_votes = db.Column(db.Float, nullable=False)
    gauge_data = db.Column(db.JSON, nullable=False)
    transaction_hash = db.Column(db.String, nullable=False)
    block_number = db.Column(db.BigInteger, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)
    date_str = db.Column(db.String, nullable=False)
    period_start = db.Column(db.BigInteger, nullable=False)
    log_index = db.Column(db.Integer)

    def to_dict(self):
        return {
            'id': self.id,
            'epoch': self.epoch,
            'total_incentives': self.total_incentives,
            'convex_amount': self.convex_amount,
            'yearn_amount': self.yearn_amount,
            'convex_votes_per_usd': self.convex_votes_per_usd,
            'yearn_votes_per_usd': self.yearn_votes_per_usd,
            'convex_votes': self.convex_votes,
            'yearn_votes': self.yearn_votes,
            'gauge_data': self.gauge_data,
            'transaction_hash': self.transaction_hash,
            'block_number': self.block_number,
            'timestamp': self.timestamp,
            'date_str': self.date_str,
            'period_start': self.period_start,
            'log_index': self.log_index
        }


class Incentive(db.Model):
    __tablename__ = 'incentives'

    id = db.Column(db.Integer, primary_key=True)
    protocol = db.Column(db.String, nullable=False)
    epoch = db.Column(db.Integer)
    total_incentives = db.Column(db.Float, nullable=False)
    votium_amount = db.Column(db.Float, nullable=False)
    votemarket_amount = db.Column(db.Float, nullable=False)
    votium_votes_per_usd = db.Column(db.Float)
    votemarket_votes_per_usd = db.Column(db.Float)
    votium_votes = db.Column(db.Float, nullable=False)
    votemarket_votes = db.Column(db.Float, nullable=False)
    gauge_data = db.Column(db.JSON, nullable=False)
    transaction_hash = db.Column(db.String, nullable=False)
    block_number = db.Column(db.BigInteger, nullable=False)
    timestamp = db.Column(db.BigInteger, nullable=False)
    date_str = db.Column(db.String, nullable=False)
    period_start = db.Column(db.BigInteger, nullable=False)
    log_index = db.Column(db.Integer)

    def to_dict(self):
        return {
            'id': self.id,
            'protocol': self.protocol,
            'epoch': self.epoch,
            'total_incentives': self.total_incentives,
            'votium_amount': self.votium_amount,
            'votemarket_amount': self.votemarket_amount,
            'votium_votes_per_usd': self.votium_votes_per_usd,
            'votemarket_votes_per_usd': self.votemarket_votes_per_usd,
            'votium_votes': self.votium_votes,
            'votemarket_votes': self.votemarket_votes,
            'gauge_data': self.gauge_data,
            'transaction_hash': self.transaction_hash,
            'block_number': self.block_number,
            'timestamp': self.timestamp,
            'date_str': self.date_str,
            'period_start': self.period_start,
            'log_index': self.log_index
        }
