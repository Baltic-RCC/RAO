import pandas as pd
import json
import uuid


PATH_ASSESSED_ELEMENTS = r"/crosa/common_assessed_element_list.xlsx"
PATH_CONTINGENCIES = r"/crosa/common_contingency_list.xlsx"
PATH_REMEDIAL_ACTIONS = r"/crosa/common_remedial_action_list.xlsx"
assessed_elements_df = pd.read_excel(PATH_ASSESSED_ELEMENTS)
contingencies_df = pd.read_excel(PATH_CONTINGENCIES)
remedial_actions_df = pd.read_excel(PATH_REMEDIAL_ACTIONS)

assessed_elements_df = assessed_elements_df[~assessed_elements_df.grid_element_id.isna()]
assessed_elements_df = assessed_elements_df[~assessed_elements_df.eq_type.isin(['transformer', 'tieline'])]

contingencies_df = contingencies_df[contingencies_df.type == "Ordinary"]
contingencies_df = contingencies_df[~contingencies_df.grid_element_id.isna()]

remedial_actions_df = remedial_actions_df[remedial_actions_df.alt_type == 'TopologyAction']


# Testing filter
# contingencies_df = contingencies_df[contingencies_df.co_name.isin(['OCO_LN317', 'OCO_LN314', 'OCO_LN457'])]
contingencies_df = contingencies_df[contingencies_df.co_name.isin(['OCO_LN317'])]


crac = {
    "type": "CRAC",
    "version": "2.4",
    "info": "Baltic common CRAC file",
    "id": "LS_unsecure",
    "name": "LS_unsecure",
    "instants": [{
        "id": "preventive",
        "kind": "PREVENTIVE"
    }, {
        "id": "outage",
        "kind": "OUTAGE"
    }, {
        "id": "curative",
        "kind": "CURATIVE"
    }],
    "ra-usage-limits-per-instant": [],
    "networkElementsNamePerId": {},
    "contingencies": [],
    "flowCnecs": [],
    "networkActions": [],
}

for ae in assessed_elements_df.to_dict('records'):
    cnec = {
        "id": ae['registered_resource'],
        "name": ae['name'],
        "networkElementId": ae['grid_element_id'],
        "operator": ae['operator'],
        "thresholds": [{
            "unit": "megawatt",
            "min": -350,
            "max": 350,
            "side": 1
        }],
        "instant": "preventive",
        "optimized": True,
        "monitored": False,
        "nominalV": [330.0],
    }
    crac['flowCnecs'].append(cnec)

    for mrid, gdf in contingencies_df.groupby("registered_resource"):
        cnec_curative = {
            "id": f"_{uuid.uuid4()}",
            "name": ae['name'],
            "networkElementId": ae['grid_element_id'],
            "operator": ae['operator'],
            "thresholds": [{
                "unit": "megawatt",
                "min": -350,
                "max": 350,
                "side": 1
            }],
            "contingencyId": mrid,
            "instant": "curative",
            "optimized": True,
            "monitored": False,
            "nominalV": [330.0],
        }
        crac['flowCnecs'].append(cnec_curative)


for mrid, gdf in contingencies_df.groupby("registered_resource"):
    contingency = {
        "id": mrid,
        "name": gdf.co_name.unique().item(),
        "networkElementsIds": gdf["grid_element_id"].to_list()
    }
    crac['contingencies'].append(contingency)

for ra in remedial_actions_df.to_dict('records'):
    remedial_action = {
      "id": ra['registered_resource'],
      "name": ra['ra_name'],
      "operator": ra['operator'],
      "onInstantUsageRules": [
        {
            "usageMethod": "available",
            "instant": "preventive"
        },
        {
            "usageMethod": "available",
            "instant": "curative"
        }
      ],
      "topologicalActions": [
        {
            "networkElementId": ra['equipment'],
            "actionType": "open"
        }
      ]
    }
    crac['networkActions'].append(remedial_action)

with open("common_baltic_crac.json", "w") as f:
    json_string = json.dumps(crac, sort_keys=False, indent=2)
    f.write(json_string)
