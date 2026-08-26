"""Reviewed exposure policy for Proposal Trace sources."""

PUBLIC = "public"
AUTHENTICATED = "authenticated"
DISABLED = "disabled"

TRACKED_SOURCE_IDS = frozenset(
    {
        "resupply:governance",
        "curve:ownership",
        "curve:parameter",
        "safe:1:0xc420c9d507d0e038bd76383aaadcad576ed0073c",
        "safe:1:0xfe11a5009f2121622271e7dd0fd470264e076af6",
        "safe:1:0xfeb4acf3df3cdea7399794d0869ef76a6efaff52",
        "safe:1:0x16388463d60ffe0661cf7f1f31a7d658ac790ff7",
    }
)

# Proposal Trace already publishes these reports publicly as Wavey Gists. The
# API exposes only the reviewed projection and therefore uses the same public
# boundary. A newly tracked source is absent here and fails closed.
SOURCE_POLICIES = {source_id: PUBLIC for source_id in TRACKED_SOURCE_IDS}


def exposure_policy(source_id):
    if set(SOURCE_POLICIES) != set(TRACKED_SOURCE_IDS):
        raise RuntimeError("Proposal Trace source policy is incomplete")
    policy = SOURCE_POLICIES.get(source_id)
    if policy not in {PUBLIC, AUTHENTICATED, DISABLED}:
        return DISABLED
    return policy
