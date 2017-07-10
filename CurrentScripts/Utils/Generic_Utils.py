def create_payload(table, sqlstmt, state = "N/A"):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': state,
    }

def capublic_format_committee_name(short_name, house):
    if house == 'CX' or house == "Assembly":
        return "Assembly Standing Committee on " + short_name
    return "Senate Standing Committee on " + short_name

def capublic_format_house(house):
    if house == "CX":
        return "Assembly"
    return "Senate"