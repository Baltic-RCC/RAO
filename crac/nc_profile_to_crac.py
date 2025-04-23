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


assessed_elements = rao_data.type_tableview("AssessedElement", string_to_number=False)
#assessed_elements_df = assessed_elements_df[~assessed_elements_df.eq_type.isin(['transformer', 'tieline'])]

contingency_equipment = rao_data.type_tableview("ContingencyEquipment", string_to_number=False)
contingencies = rao_data.key_tableview("Contingency.EquipmentOperator", string_to_number=False)
contingencies = contingencies.merge(contingency_equipment, left_on="IdentifiedObject.mRID", right_on="ContingencyElement.Contingency", suffixes=("_ContingencyElement", "_ContingencyEquipment"))


remedial_actions = rao_data.type_tableview("TopologyAction", string_to_number=False)


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

for ra in remedial_actions.to_dict('records'):
    remedial_action = {
      "id": ra['IdentifiedObject.mRID'],
      "name": ra['IdentifiedObject.name'],
      "operator": "",  # TODO - get from EQ, if needed?
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
            "networkElementId": ra["TopologyAction.Equipment"],
            "actionType": "open"  # TODO - operation mapping
        }
      ]
    }
    crac['networkActions'].append(remedial_action)

with open(EXPORT_CRAC_NAME, "w") as file_object:
    json.dump(crac, file_object, sort_keys=False, indent=2)

