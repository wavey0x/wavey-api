#!/usr/bin/env python3
"""Compare every migrated API record with its frozen PostgreSQL source.

Uses a Flask test application and database-only model reads. It does not load
the production app, contact RPC providers, or invoke a notification transport.
"""

import argparse
from collections import Counter, defaultdict
from decimal import Decimal
import gzip
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from flask import Flask
from sqlalchemy import select
from sqlalchemy.engine import URL
from database import db,init_app
from models import Stake,CrvLlHarvest,GaugeVoteInfo,Incentive,UserInfo,GlobalWeekInfo,UserWeekInfo


MODELS = [Stake,CrvLlHarvest,GaugeVoteInfo,Incentive,UserInfo,GlobalWeekInfo,UserWeekInfo]


def typed_source(column,value):
    if value is None:
        return None
    kind=column['type']
    if kind=='numeric':
        return Decimal(value)
    if kind in ('int2','int4','int8'):
        return int(value)
    if kind=='float8':
        return float(value)
    if kind=='bool':
        return value=='true'
    if kind in ('json','jsonb'):
        return json.loads(value)
    return value


def rendered(model,values):
    instance=model(**values)
    if model is CrvLlHarvest:
        result={key:getattr(instance,key) for key in ('id','timestamp','name','underlying',
            'compounder','block','txn_hash','date_str')}
        result['profit']=str(instance.profit)
        return result
    return instance.to_dict()


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--database',type=Path,required=True)
    args=parser.parse_args()
    wanted={model.__tablename__ for model in MODELS}-{ 'user_week_info' }
    source_rows=defaultdict(list)
    with gzip.open(args.snapshot,'rt',encoding='utf-8') as source:
        header=json.loads(source.readline())
        columns={table['name']:table['columns'] for table in header['tables']}
        prefixes=tuple('{"kind":"row","table":'+json.dumps(name)+',' for name in wanted)
        for line in source:
            # The exporter fixes this record prefix. Avoid parsing the millions
            # of progress rows that the API never consumes.
            if not line.startswith(prefixes):
                continue
            record=json.loads(line)
            name=record['table']
            source_rows[name].append({c['name']:typed_source(c,v)
                for c,v in zip(columns[name],record['values'])})
    weeks=defaultdict(list)
    for row in source_rows['week_info']:
        if row['week_id'] is not None and row['token'] is not None:
            weeks[(row['week_id'],row['token'])].append(row)
    for user in source_rows['user_info']:
        for week in weeks.get((user['week_id'],user['token']),[]):
            joined={key:user[key] for key in ('account','week_id','ybs')}
            joined.update({'user_'+key:user[key] for key in
                ('weight','balance','boost','stake_map','rewards_earned')})
            joined.update({'global_weight':week['weight'],'global_stake_map':week['stake_map']})
            joined.update({key:week[key] for key in ('token','start_ts','end_ts','start_block',
                'end_block','start_time_str','end_time_str')})
            source_rows['user_week_info'].append(joined)
    snapshot_hash=hashlib.sha256(args.snapshot.read_bytes()).hexdigest()
    app=Flask(__name__)
    app.testing=True
    app.config.update(SQLALCHEMY_DATABASE_URI=URL.create('sqlite+pysqlite',
        database=args.database.resolve().as_uri(),query={'mode':'ro','uri':'true'}),
        YEARN_IMPORT_SHA256=snapshot_hash)
    init_app(app,allow_rehearsal=True)
    results={}
    with app.app_context():
        for model in MODELS:
            properties=[(p.key,p.columns[0].name) for p in model.__mapper__.column_attrs]
            expected=Counter(digest(rendered(model,{key:row[column] for key,column in properties}))
                             for row in source_rows[model.__tablename__])
            actual=Counter()
            # Column projections retain every row, including any source rows
            # sharing an incomplete legacy ORM identity key.
            statement=select(*(getattr(model,key) for key,_column in properties))
            for row in db.session.execute(statement):
                actual[digest(rendered(model,dict(zip((key for key,_ in properties),row))))]+=1
            if actual != expected:
                raise RuntimeError('API serialization differs from the frozen source: '+model.__tablename__)
            results[model.__tablename__]=sum(actual.values())
    print(json.dumps({'status':'verified','source_sha256':snapshot_hash,'api_records':results},indent=2))


if __name__=='__main__':
    main()
