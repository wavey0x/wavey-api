from models import Stake


def test_stake_legacy_attributes_map_to_current_database_columns():
    assert Stake.newweight.property.columns[0].name == "new_weight"
    assert Stake.staked.property.columns[0].name == "is_stake"
