import pandas
import triplets
import json
import uuid


EXPORT_CRAC_NAME = "TC1_example_crac.json"

PATH_ASSESSED_ELEMENTS = r"../test-data/tests/test-data/TC1_assessed_elements.xml"
PATH_CONTINGENCIES = r"../test-data/tests/test-data/TC1_contingencies.xml"
PATH_REMEDIAL_ACTIONS = r"../test-data/tests/test-data/TC1_remedial_actions.xml"


rao_data = pandas.read_RDF([PATH_ASSESSED_ELEMENTS, PATH_CONTINGENCIES, PATH_REMEDIAL_ACTIONS])

for key, value in rao_data.types_dict().items():
    print(f"Loaded {value} {key}")

# Assessed Elements
assessed_elements = rao_data.type_tableview("AssessedElement", string_to_number=False)

# Contingencies
contingency_equipment = rao_data.type_tableview("ContingencyEquipment", string_to_number=False)
contingencies = rao_data.key_tableview("Contingency.EquipmentOperator", string_to_number=False)
contingencies = contingencies.merge(contingency_equipment, left_on="IdentifiedObject.mRID", right_on="ContingencyElement.Contingency", suffixes=("_ContingencyElement", "_ContingencyEquipment"))

# Remedial Actions
grid_state_alteration = rao_data.key_tableview("GridStateAlteration.GridStateAlterationRemedialAction", string_to_number=False)
remedial_actions = rao_data.type_tableview("GridStateAlterationRemedialAction", string_to_number=False)
remedial_actions = remedial_actions.merge(grid_state_alteration, left_on="IdentifiedObject.mRID", right_on="GridStateAlteration.GridStateAlterationRemedialAction", suffixes=("_GridStateAlterationRemedialAction", "_GridStateAlteration"))


crac = {
    "type": "CRAC",
    "version": "2.4",
    "info": "TC1 CRAC Example",
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

# Assessed elements for preventive
for ae in assessed_elements.to_dict('records'):
    cnec = {
        "id": ae['IdentifiedObject.mRID'],
        "name": ae['IdentifiedObject.name'],
        "networkElementId": ae['AssessedElement.ConductingEquipment'],
        "operator": ae['AssessedElement.AssessedSystemOperator'],
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

# Assessed element for each contingency defined
    for contingency_mRID in rao_data.query("KEY == 'ContingencyElement.Contingency'").VALUE.unique():

        cnec_curative = {
            "id": f"{ae['IdentifiedObject.mRID']}_{contingency_mRID}",
            "name": ae['IdentifiedObject.name'],
            "networkElementId": ae['AssessedElement.ConductingEquipment'],
            "operator": ae['AssessedElement.AssessedSystemOperator'],
            "thresholds": [{
                "unit": "megawatt",
                "min": -350,
                "max": 350,
                "side": 1
            }],
            "contingencyId": contingency_mRID,
            "instant": "curative",
            "optimized": True,
            "monitored": False,
            "nominalV": [330.0],
        }
        crac['flowCnecs'].append(cnec_curative)

# Add contingencies
for contingency_mRID, contingency_data in contingencies.groupby("IdentifiedObject.mRID_ContingencyElement"):
    contingency = {
        "id": contingency_mRID,
        "name": contingency_data["IdentifiedObject.name_ContingencyElement"].iloc[0],
        "networkElementsIds": contingency_data['ContingencyEquipment.Equipment'].to_list()
    }
    crac['contingencies'].append(contingency)

for remedial_action_mRID, remedial_action_data in remedial_actions.groupby("IdentifiedObject.mRID_GridStateAlterationRemedialAction"):

    remedial_action = {
      "id": remedial_action_data['IdentifiedObject.mRID_GridStateAlterationRemedialAction'].iloc[0],
      "name": remedial_action_data['IdentifiedObject.name_GridStateAlterationRemedialAction'].iloc[0],
      "operator": remedial_action_data['RemedialAction.RemedialActionSystemOperator'].iloc[0],
      "onInstantUsageRules": [
        # {
        #     "usageMethod": "available",
        #     "instant": "preventive"
        # },
        # {
        #     "usageMethod": "available",
        #     "instant": "curative"
        # },
        {
             "usageMethod": "available",
             "instant": remedial_action_data["RemedialAction.kind"].iloc[0].split(".")[-1]
        }
      ],
      "topologicalActions": [

      ]
    }

    for action in remedial_action_data.to_dict("records"):

        topology_action_id = action.get("TopologyAction.Equipment")

        if topology_action_id:
            remedial_action["topologicalActions"].append(
                {
                    "networkElementId": topology_action_id,
                    "actionType": "open"  # TODO - operation mapping
                }
        )


    crac['networkActions'].append(remedial_action)

with open(EXPORT_CRAC_NAME, "w") as file_object:
    json.dump(crac, file_object, sort_keys=False, indent=2)
    print(f"Created {EXPORT_CRAC_NAME}")

