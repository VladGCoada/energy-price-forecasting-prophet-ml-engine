"""Project constants shared across modules."""

RENEWABLE_TYPES = {
    "Solar",
    "Wind Offshore",
    "Wind Onshore",
    "Hydro Water Reservoir",
    "Hydro Run-of-river and poundage",
    "Biomass",
    "Geothermal",
}

ENTSOE_BASE_URL = "https://web-api.tp.entsoe.eu/api"

COUNTRY_EIC_CODES = {
    "DE": "10Y1001A1001A83F",
    "NL": "10YNL----------L",
    "DK1": "10YDK-1--------W",
    "DK2": "10YDK-2--------M",
    "FR": "10YFR-RTE------C",
    "BE": "10YBE----------2",
    "RO": "10YRO-TEL------P",
    "HU": "10YHU-MAVIR----U",
}
