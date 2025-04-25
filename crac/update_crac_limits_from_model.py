import pandas
import triplets
import json


CRAC_NAME = "TC1_example_crac.json"

models = [r"../test-data/tests/test-data/TC1_CGMES.zip"]

def get_limits(data):

    # Get Limit Sets
    limits = data.type_tableview('OperationalLimitSet', string_to_number=False).reset_index()

    # Add OperationalLimits
    limits = limits.merge(data.key_tableview('OperationalLimit.OperationalLimitSet').reset_index(), left_on='ID', right_on='OperationalLimit.OperationalLimitSet', suffixes=("_OperationalLimitSet", "_OperationalLimit"))

    # Add LimitTypes
    limits = limits.merge(data.type_tableview("OperationalLimitType", string_to_number=False).reset_index(), right_on="ID", left_on="OperationalLimit.OperationalLimitType")

    # Add link to equipment via Terminals
    limits = limits.merge(data.type_tableview('Terminal', string_to_number=False).reset_index(), left_on="OperationalLimitSet.Terminal", right_on="ID", suffixes=("", "_Terminal"))

    limits["ID_Equipment"] = None

    # Get Equipment via terminal -> 'OperationalLimitSet.Terminal' -> 'Terminal.ConductingEquipment'
    if 'Terminal.ConductingEquipment' in limits.columns:
        limits["ID_Equipment"] = limits["ID_Equipment"].fillna(limits["Terminal.ConductingEquipment"])

    # Get Equipment directly -> 'OperationalLimitSet.Equipment'
    if 'OperationalLimitSet.Equipment' in limits.columns:
        limits["ID_Equipment"] = limits["ID_Equipment"].fillna(limits['OperationalLimitSet.Equipment'])

    # Add equipment type
    #limits = limits.merge(data.query("KEY == 'Type'"), left_on="ID_Equipment", right_on="ID", suffixes=("", "_Type"))

    return limits

data = pandas.read_RDF(models)

limits = get_limits(data)

# Get voltages on terminals to convert A limits to MW
limits = limits.merge(data.type_tableview("SvVoltage"), left_on="Terminal.TopologicalNode", right_on="SvVoltage.TopologicalNode", suffixes=("", "_SvVoltage"))

# Ensure that Active Power Limits column would be present
if "ActivePowerLimit.value" not in limits.columns:
    limits["ActivePowerLimit.value"] = pandas.NA

# Compute MW approximation where ActivePowerLimit is NaN and Current/Voltage are available
if "CurrentLimit.value" in limits.columns and "SvVoltage.v" in limits.columns:
    condition = limits["ActivePowerLimit.value"].isna() & limits["CurrentLimit.value"].notna() & limits["SvVoltage.v"].notna()
    # Calculate MW and assign
    limits.loc[condition, "ActivePowerLimit.value"] = round(3**0.5 * limits.loc[condition, "CurrentLimit.value"] * limits.loc[condition, "SvVoltage.v"] / 1000, 1)


patl_limits = limits[limits["OperationalLimitType.kind"].str.endswith(".patl")].groupby("ID_Equipment")
tatl_limits = limits[limits["OperationalLimitType.kind"].str.endswith(".tatl")].groupby("ID_Equipment")

# Generate mean voltages for equipment
voltages = patl_limits["SvVoltage.v"].mean().round(1).to_dict()

patl_current_limits = {}
tatl_current_limits = {}
if "CurrentLimit.value" in limits.columns:
    patl_current_limits = patl_limits["CurrentLimit.value"].min().to_dict()
    tatl_current_limits = tatl_limits["CurrentLimit.value"].min().to_dict()

patl_power_limits = {}
tatl_power_limits = {}
if "ActivePowerLimit.value" in limits.columns:
    patl_power_limits = patl_limits["ActivePowerLimit.value"].min().to_dict()
    tatl_power_limits = tatl_limits["ActivePowerLimit.value"].min().to_dict()


if isinstance(CRAC_NAME, str):
   with open(CRAC_NAME, "r") as file_object:
       crac = json.load(file_object)

for position, monitored_element in enumerate(crac['flowCnecs']):

    # Set nominal voltage to operational voltages, taken from SV
    if operational_voltage := voltages.get(monitored_element['networkElementId']):
        crac['flowCnecs'][position]['nominalV'] = [operational_voltage]

    current_limits = patl_current_limits
    power_limits = patl_power_limits

    if monitored_element["instant"] == "curative":

        current_limits = tatl_current_limits
        power_limits = tatl_power_limits

    if limit := power_limits.get(monitored_element['networkElementId']):
        unit = "megawatt"
    elif limit := current_limits.get(monitored_element['networkElementId']):
        unit = "ampere"
    else:
        print(f"Limit not found for {monitored_element['networkElementId']}")
        continue

    crac['flowCnecs'][position]['thresholds'] = [{'max': limit, 'min': limit *-1, 'side': 1, 'unit': unit}]


with open(CRAC_NAME, "w") as file_object:
    json.dump(crac, file_object, sort_keys=False, indent=2)
    print(f"Updated Flow limits and Nominal Voltages in {CRAC_NAME}")


