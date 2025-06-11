from loguru import logger
import triplets
import uuid
from datetime import datetime
from pika import BasicProperties
import pandas as pd
from io import BytesIO
from crac import models
import json


class CracBuilder:
    """
    PreProcessor class for handling pre-processing tasks.
    This class is a placeholder and can be extended with specific pre-processing methods.
    """

    def __init__(self, data: pd.Dataframe):
        logger.info(f"PreProcessor initialized with configuration")
        self.data = data
        self._crac = None

    @property
    def crac(self):
        if self._crac is None:
            logger.error("CRAC model is not built yet. Please call build_crac() method first.")
            return None
        return self._crac.model_dump(exclude_none=True, by_alias=True)

    @property
    def crac_pprint(self):
        return print(json.dumps(self.crac, indent=2))

    def process_contingencies(self, specific_contingencies: list | None = None):

        contingency_equipment = self.data.type_tableview("ContingencyEquipment", string_to_number=False)
        contingencies = self.data.key_tableview("Contingency.EquipmentOperator", string_to_number=False)
        contingencies = contingencies.merge(contingency_equipment,
                                            left_on="IdentifiedObject.mRID",
                                            right_on="ContingencyElement.Contingency",
                                            suffixes=("_ContingencyElement", "_ContingencyEquipment"))

        # Filter contingencies if specific_contingencies is provided
        if specific_contingencies:
            contingencies = contingencies[contingencies['IdentifiedObject.mRID_ContingencyElement'].isin(specific_contingencies)]
            if contingencies.empty:
                logger.warning(f"No contingencies found for specified IDs: {specific_contingencies}")
                return

        for mrid, data in contingencies.groupby("IdentifiedObject.mRID_ContingencyElement"):
            contingency = models.Contingency(
                id=mrid,
                name=data["IdentifiedObject.name_ContingencyElement"].iloc[0],
                networkElementsIds=data['ContingencyEquipment.Equipment'].to_list()
            )
            self._crac.contingencies.append(contingency)

    def process_cnecs(self):
        """
        We want to always monitor all assessed elements, so we create CNECs for each assessed element.
        This process always looks in already defined contingencies to have in synchronized.
        """

        assessed_elements = self.data.type_tableview("AssessedElement", string_to_number=False)

        for ae in assessed_elements.to_dict('records'):

            # Exclude assessed elements which normalEnabled = false
            if ae.get('AssessedElement.normalEnabled', 'true') == 'false':
                logger.warning(f"Assessed element excluded due to normalEnabled is false: {ae['IdentifiedObject.name']}")
                continue

            # Exclude assessed elements if attribute inBaseCase is false
            if ae.get("AssessedElement.inBaseCase", "false").lower() == "false":
                logger.warning(f"Assessed element excluded due to inBaseCase is false: {ae['IdentifiedObject.name']}")
                continue

            # Define whether element secured/scanned
            _secured = bool(ae.get("AssessedElement.SecuredForRegion", False))
            _scanned = bool(ae.get("AssessedElement.ScannedForRegion", False))

            # Create CNEC for each assessed element
            cnec_preventive = models.FlowCnec(
                id=ae['IdentifiedObject.mRID'],
                name=ae['IdentifiedObject.name'],
                networkElementId=ae['AssessedElement.ConductingEquipment'],
                operator=ae['AssessedElement.AssessedSystemOperator'],
                thresholds=[models.Threshold()],
                instant="preventive",
                optimized=_secured,
                monitored=_scanned,
            )
            self._crac.flowCnecs.append(cnec_preventive)

            # Include curative CNEC for each contingency defined
            for contingency in self._crac.contingencies:
                cnec_curative = cnec_preventive.model_copy(update={"contingencyId": contingency.id, "instant": "curative"})
                self._crac.flowCnecs.append(cnec_curative)


    def process_remedial_actions(self):

        # Grid state alteration remedial actions
        grid_state_alteration = self.data.key_tableview("GridStateAlteration.GridStateAlterationRemedialAction",
                                                        string_to_number=False)
        remedial_actions = self.data.type_tableview("GridStateAlterationRemedialAction", string_to_number=False)
        remedial_actions = remedial_actions.merge(grid_state_alteration,
                                                  left_on="IdentifiedObject.mRID",
                                                  right_on="GridStateAlteration.GridStateAlterationRemedialAction",
                                                  suffixes=("_GridStateAlterationRemedialAction", "_GridStateAlteration"))
        property_ranges = self.data.type_tableview("StaticPropertyRange", string_to_number=False)

        for mrid, data in remedial_actions.groupby("IdentifiedObject.mRID_GridStateAlterationRemedialAction"):

            # Create network actions objects
            actions = []
            for action in data.to_dict("records"):

                # Get type of alteration action
                action_type = action.get("Type_GridStateAlteration", None)
                if action_type is None:
                    logger.warning(f"Grid state alteration type is unknown or not supported: {action['IdentifiedObject.mRID_GridStateAlteration']}")
                    continue

                # Get relevant property ranges for the action
                ranges = property_ranges[property_ranges['RangeConstraint.GridStateAlteration'] == action['IdentifiedObject.mRID_GridStateAlteration']]
                if ranges.empty:
                    logger.warning(f"No relevant property ranges found for {action_type}: {action['IdentifiedObject.mRID_GridStateAlteration']}")
                    logger.warning(f"Using default property range value: 0")
                    normal_value = 0
                else:
                    # TODO Need support if multiple property ranges are defined
                    # Use the first range's normal value
                    normal_value = ranges.iloc[0]['RangeConstraint.normalValue']

                # TODO create a mapping table of supported action type
                # TODO give normal value under generic name like "normalValue" to models and handle with alias
                if action_type == 'TopologyAction':
                    # TODO map different types of actions, depending on current state of equipment (might need SIS NC profile implementation)
                    element_id = action.get("TopologyAction.Equipment")
                    referenced_action = models.TerminalsAction
                elif action_type == 'ShuntCompensatorModification':
                    element_id = action.get("ShuntCompensatorModification.ShuntCompensator")
                    referenced_action = models.ShuntCompensatorPositionAction
                else:
                    logger.warning(f"Grid state alteration type is not supported: {action_type}")
                    continue

                # Create action object
                action = referenced_action(networkElementId=element_id, normalValue=normal_value)
                actions.append(action)

            # Create network action object if any of the actions were created
            if not actions:
                logger.warning(f"No actions available for GridStateAlterationRemedialAction: {mrid}")
                continue
            network_action = models.NetworkAction(
                id=data['IdentifiedObject.mRID_GridStateAlterationRemedialAction'].iloc[0],
                name=data['IdentifiedObject.name_GridStateAlterationRemedialAction'].iloc[0],
                operator=data['RemedialAction.RemedialActionSystemOperator'].iloc[0],
                onInstantUsageRules=[
                    {
                        "usageMethod": "available",
                        "instant": data["RemedialAction.kind"].iloc[0].split(".")[-1]
                    }
                ],
                terminalsConnectionActions=[i for i in actions if isinstance(i, models.TerminalsAction)],
                shuntCompensatorPositionActions=[i for i in actions if isinstance(i, models.ShuntCompensatorPositionAction)],
            )
            self._crac.networkActions.append(network_action)

    def build_crac(self, contingency_ids: list | None = None):

        # Initialize CRAC object
        self._crac = models.Crac()  # TODO can be replaced with separate function also need to include some general parameters

        # Process contingencies, CNECs and remedial actions
        self.process_contingencies(specific_contingencies=contingency_ids)
        self.process_cnecs()
        self.process_remedial_actions()

        return self.crac


if __name__ == '__main__':
    # Test files
    contingencies = r"../test-data/TC1_contingencies.xml"
    assessed_elements = r"../test-data/TC1_assessed_elements.xml"
    remedial_actions = r"../test-data/TC1_remedial_actions.xml"

    # Load data into triplets
    data = pd.read_RDF([contingencies, assessed_elements, remedial_actions])

    # Create instance
    service = CracBuilder(data=data)
    crac = service.build_crac()
