# backend/machine_map.py - Support broader machine range
TOPIC_TO_MACHINE = {
    "COUNT1": (2, 1),
    "COUNT2": (2, 6),
    "COUNT3": (2, 16),
    "COUNT4": (2, 41),
    # Plant 1 mappings remain same
    "COUNT5(JJ5)": (1, 31), "COUNT6(JJ6)": (1, 26), "COUNT7(JJ7)": (1, 40),
    "COUNT8(JJ8)": (1, 46), "COUNT9(JJ9)": (1, 54), "COUNT10(JJ10)": (1, 36),
    "COUNT11(JJ11)": (1, 5), "COUNT12(JJ12)": (1, 4), "COUNT13(JJ13)": (1, 3),
    "COUNT14(JJ14)": (1, 2), "COUNT15(JJ15)": (1, 1),
}

# COUNT52 supports any machine number parsed from payload
COUNT52_GROUP = {
    "plant": 2,
    "machines": list(range(1, 100))  # Support any machine 1-99
}

SPECIAL_RULES = {
    "COUNT10(JJ10)": "rule_count10",
}

def rule_count10(val):
    return 0, round(val % 1000, 2)
