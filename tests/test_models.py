from models import Stake


def test_stake_newweight_maps_to_current_database_column():
    assert Stake.newweight.property.columns[0].name == "new_weight"
