from loguru import logger
import pypowsybl
import pandas as pd

"""
Helper script used to retrieve generation units that are created when importing CGMES network with "iidm.import.cgmes.convert-boundary": "True"}
"""

COUNTRIES_TO_COUNTERTRADE = ("EE", "LV", "LT") # Include only our SOR countries
MIN_NOMINAL_V = 330 # kV
BOUNDARY_VOLTAGE_LEVEL_NAME = "boundary"

def find_boundary_generators(network):
    voltage_levels = network.get_voltage_levels(attributes=["name", "nominal_v", "substation_id"])
    voltage_levels = voltage_levels[(voltage_levels["name"] == BOUNDARY_VOLTAGE_LEVEL_NAME) & (voltage_levels["nominal_v"] >= MIN_NOMINAL_V)]
    voltage_levels = filter_by_country(network, voltage_levels)

    # boundary line attached to each boundary voltage level - label for the boundary generator
    lines = network.get_lines()
    side1 = lines.reset_index().rename(columns={"voltage_level1_id": "boundary_voltage_level", "voltage_level2_id": "grid_voltage_level"})
    side2 = lines.reset_index().rename(columns={"voltage_level2_id": "boundary_voltage_level", "voltage_level1_id": "grid_voltage_level"})
    boundary_lines = (pd.concat([side1, side2]).loc[lambda df: df["boundary_voltage_level"].isin(voltage_levels.index)].drop_duplicates("boundary_voltage_level").set_index("boundary_voltage_level"))

    # Remove stagnant boundary lines (either leftover non-relevant equivalent injections or cross-border lines in outage)
    boundary_lines = boundary_lines[boundary_lines["connected1"] & boundary_lines["connected2"]]

    # Keep only generators that are on remaining boundary lines
    generators = network.get_generators()
    generators = generators[generators["voltage_level_id"].isin(boundary_lines.index)].copy()

    # Keep only boundary injection generation units (equivalent injections have target_p preset, negative when exporting)
    generators = generators[generators["target_p"] != 0].copy()

    # Set boundary generation unit names
    generators["country"] = generators["voltage_level_id"].map(voltage_levels["country"])
    generators["line_id"] = generators["voltage_level_id"].map(boundary_lines["id"])
    generators["line_name"] = generators["voltage_level_id"].map(boundary_lines["name"])
    generators["grid_voltage_level"] = generators["voltage_level_id"].map(boundary_lines["grid_voltage_level"])
    return generators

def filter_by_country(network, voltage_levels, countries=COUNTRIES_TO_COUNTERTRADE):
    """Keep boundary lines only for substation IDs which are in our CROSA scope (COUNTRIES_TO_COUNTERTRADE)"""
    subs = network.get_substations(attributes=["country"])
    voltage_levels = voltage_levels.assign(country=voltage_levels["substation_id"].map(subs["country"]))
    return voltage_levels[voltage_levels["country"].isin(countries)]

def find_boundary_lines(network) -> set[str]:
    """
    Ids (without leading '_') of the lines at the boundary, all countries:
        - boundary (dangling) lines, also when paired into a tie line, and the tie lines
        - with convert-boundary: lines ending on a boundary voltage level (BOUNDARY_VOLTAGE_LEVEL_NAME)
    """
    ids = set(network.get_boundary_lines(attributes=[]).index) | set(network.get_tie_lines(attributes=[]).index)
    voltage_levels = network.get_voltage_levels(attributes=["name"])
    boundary_voltage_levels = voltage_levels.index[voltage_levels["name"] == BOUNDARY_VOLTAGE_LEVEL_NAME]
    lines = network.get_lines(attributes=["voltage_level1_id", "voltage_level2_id"])
    ids |= set(lines.index[lines["voltage_level1_id"].isin(boundary_voltage_levels)
                           | lines["voltage_level2_id"].isin(boundary_voltage_levels)])
    return {element_id.lstrip("_") for element_id in ids}

