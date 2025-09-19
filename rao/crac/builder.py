from loguru import logger
import pandas as pd
import triplets
from rao.crac import models
import json
from common.decorators import performance_counter


class CracBuilder:
    """
    PreProcessor class for handling pre-processing tasks.
    This class is a placeholder and can be extended with specific pre-processing methods.
    """

    def __init__(self, data: pd.DataFrame, network: pd.DataFrame | None):
        logger.info(f"CRAC builder initialized")
        self.data = data
        self.network = network
        self.limits = None
        self._crac = None

        # TODO [TEMPORARY] exclude boundary set
        boundary_files = self.network[(self.network.KEY == 'label') & (self.network.VALUE.str.contains("ENTSOE"))]
        self.network = self.network[~self.network.INSTANCE_ID.isin(boundary_files.INSTANCE_ID)]

    @property
    def crac(self):
        if self._crac is None:
            logger.error("CRAC model is not built yet. Please call build_crac() method first.")
            return None
        return self._crac.model_dump(exclude_none=True, by_alias=True)

    @property
    def crac_pprint(self):
        return print(json.dumps(self.crac, indent=2))

    def get_limits(self):

        if self.network is None:
            logger.error("Network model is not provided. Cannot retrieve limits.")
            return

        logger.info(f"Retrieving operational limits from network model")

        # Get Limit Sets
        limits = self.network.type_tableview('OperationalLimitSet', string_to_number=False).reset_index()

        # Add OperationalLimits
        limits = limits.merge(self.network.key_tableview('OperationalLimit.OperationalLimitSet').reset_index(),
                              left_on='ID',
                              right_on='OperationalLimit.OperationalLimitSet',
                              suffixes=("_OperationalLimitSet", "_OperationalLimit"))

        # Add LimitTypes
        limits = limits.merge(self.network.type_tableview("OperationalLimitType", string_to_number=False).reset_index(),
                              right_on="ID", left_on="OperationalLimit.OperationalLimitType")

        # Add link to equipment via Terminals
        limits = limits.merge(self.network.type_tableview('Terminal', string_to_number=False).reset_index(),
                              left_on="OperationalLimitSet.Terminal", right_on="ID", suffixes=("", "_Terminal"))

        limits["ID_Equipment"] = None

        # Get Equipment via terminal -> 'OperationalLimitSet.Terminal' -> 'Terminal.ConductingEquipment'
        if 'Terminal.ConductingEquipment' in limits.columns:
            limits["ID_Equipment"] = limits["ID_Equipment"].fillna(limits["Terminal.ConductingEquipment"])

        # Get Equipment directly -> 'OperationalLimitSet.Equipment'
        if 'OperationalLimitSet.Equipment' in limits.columns:
            limits["ID_Equipment"] = limits["ID_Equipment"].fillna(limits['OperationalLimitSet.Equipment'])

        # Add equipment type
        # limits = limits.merge(data.query("KEY == 'Type'"), left_on="ID_Equipment", right_on="ID", suffixes=("", "_Type"))

        # Ensure that Active Power Limits column would be present
        if "ActivePowerLimit.value" not in limits.columns:
            limits["ActivePowerLimit.value"] = pd.NA

        # Get voltages on terminals to convert A limits to MW
        limits = limits.merge(self.network.type_tableview("SvVoltage"), left_on="Terminal.TopologicalNode",
                              right_on="SvVoltage.TopologicalNode", suffixes=("", "_SvVoltage"))

        # Compute MW approximation where ActivePowerLimit is NaN and Current/Voltage are available
        if "CurrentLimit.value" in limits.columns and "SvVoltage.v" in limits.columns:
            condition = limits["ActivePowerLimit.value"].isna() & limits["CurrentLimit.value"].notna() & limits["SvVoltage.v"].notna()
            # Calculate MW and assign
            limits.loc[condition, "ActivePowerLimit.value"] = round(
                ((3 ** 0.5) * limits.loc[condition, "CurrentLimit.value"] * limits.loc[condition, "SvVoltage.v"]) / 1000, 1)

        self.limits = limits

    def update_limits_from_network(self,):

        if self.limits is None:
            self.get_limits()

        logger.info(f"Updating operational limits on CNECs from network model")

        patl_limits = self.limits[self.limits["OperationalLimitType.limitType"].str.endswith(".patl")].groupby("ID_Equipment")
        tatl_limits = self.limits[self.limits["OperationalLimitType.limitType"].str.endswith(".tatl")].groupby("ID_Equipment")

        # Generate mean and max voltages for equipment
        voltages = patl_limits["SvVoltage.v"].mean().round(1).to_dict()
        max_voltage = patl_limits["SvVoltage.v"].max().round(1).to_dict()

        patl_current_limits = {}
        tatl_current_limits = {}
        if "CurrentLimit.value" in self.limits.columns:
            patl_current_limits = patl_limits["CurrentLimit.value"].min().to_dict()
            tatl_current_limits = tatl_limits["CurrentLimit.value"].min().to_dict()

        patl_power_limits = {}
        tatl_power_limits = {}
        if "ActivePowerLimit.value" in self.limits.columns:
            patl_power_limits = patl_limits["ActivePowerLimit.value"].min().to_dict()
            tatl_power_limits = tatl_limits["ActivePowerLimit.value"].min().to_dict()

        for monitored_element in self._crac.flowCnecs:

            # TODO figure out optimization that same CNEC on preventive and curative instance would be updated

            # Set nominal voltage to operational voltages, taken from SV
            if operational_voltage := voltages.get(monitored_element.networkElementId):
                # TODO add both sides of the equipment when building CRAC nominal voltages
                if "_AT" in monitored_element.name:
                    max_op_voltage = max_voltage.get(monitored_element.networkElementId)
                    monitored_element.nominalV = [max_op_voltage]
                    logger.debug(f"Flow CNEC {monitored_element.name} [{monitored_element.instant}] max operational voltage selected: {max_voltage}")
                else:
                    monitored_element.nominalV = [operational_voltage]
                    logger.debug(f"Flow CNEC {monitored_element.name} [{monitored_element.instant}] nominal voltage updated: {operational_voltage}")
            # Set limits for preventative state
            current_limits = patl_current_limits
            power_limits = patl_power_limits

            # Set limits for curative state
            if monitored_element.instant == "curative":
                current_limits = tatl_current_limits.copy()
                power_limits = tatl_power_limits.copy()

            # Fallback logic for missing curative state TATL limits
            #TODO temporary fix, TATL must be accurately represented in the models
            if power_limits.get(monitored_element.networkElementId) is None:
                fallback_limit = patl_power_limits.get(monitored_element.networkElementId)
                if fallback_limit is not None:
                    power_limits[monitored_element.networkElementId] = fallback_limit
                    logger.warning(f"TATL power limit is missing for {monitored_element.name}, using PATL value instead")

            if current_limits.get(monitored_element.networkElementId) is None:
                fallback_limit = patl_current_limits.get(monitored_element.networkElementId)
                if fallback_limit is not None:
                    current_limits[ monitored_element.networkElementId ] = fallback_limit
                    logger.warning(f"TATL current limit is missing for {monitored_element.name}, using PATL value instead")

            if limit := power_limits.get(monitored_element.networkElementId):
                unit = "megawatt"
            elif limit := current_limits.get(monitored_element.networkElementId):
                unit = "ampere"
            else:
                logger.warning(f"Limit not found for {monitored_element.name} with element id: {monitored_element.networkElementId}")
                continue

            # Set update thresholds (limits)
            monitored_element.thresholds = [models.Threshold(max=limit, min=limit * -1, side=1, unit=unit)]

    def process_contingencies(self, specific_contingencies: list | None = None):

        contingency_equipment = self.data.type_tableview("ContingencyEquipment", string_to_number=False)
        contingencies = self.data.key_tableview("Contingency.normalMustStudy", string_to_number=False)
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
            name = data["IdentifiedObject.name_ContingencyElement"].iloc[0]
            contingency_type = data["Type_ContingencyElement"].iloc[0]

            # TODO [TEMPORARY] - perform consistency check
            if not all(data['ContingencyEquipment.Equipment'].isin(self.network.ID)):
                logger.warning(f"At least one of the contingency equipment does not exist in network model: {name}")

            contingency = models.Contingency(
                id=mrid,
                name=name,
                networkElementsIds=data['ContingencyEquipment.Equipment'].to_list()
            )
            self._crac.contingencies.append(contingency)
            logger.debug(f"Added contingency of type {contingency_type}: {name}")

    def process_cnecs(self):
        """
        We want to always monitor all assessed elements, so we create CNECs for each assessed element.
        This process always looks in already defined contingencies to have in synchronized.
        """

        assessed_elements = self.data.type_tableview("AssessedElement", string_to_number=False)

        # TODO [TEMPORARY] - perform consistency check
        missing = assessed_elements[~assessed_elements['AssessedElement.ConductingEquipment'].isin(self.network.ID)]
        for _, row in missing.iterrows():
            logger.warning(f"Assessed element does not exist in network model: {row['IdentifiedObject.name']}")
        assessed_elements = assessed_elements.drop(index=missing.index)

        for ae in assessed_elements.to_dict('records'):

            # Exclude assessed elements which normalEnabled = false
            if ae.get('AssessedElement.normalEnabled', 'false') == 'false':
                logger.warning(f"Assessed element excluded due to 'normalEnabled' is false or missing: {ae['IdentifiedObject.name']}")
                continue

            # Get flag whether assessed element should be included in preventive state
            in_base_case = ae.get("AssessedElement.inBaseCase", "false").lower() == 'true'

            # Define whether element secured/scanned
            secured = bool(ae.get("AssessedElement.SecuredForRegion", False))
            scanned = bool(ae.get("AssessedElement.ScannedForRegion", False))

            # Create CNEC object for assessed element
            cnec = models.FlowCnec(
                id=f"{ae['IdentifiedObject.mRID']}",
                name=ae['IdentifiedObject.name'],
                description=ae['IdentifiedObject.description'] or "",
                networkElementId=ae['AssessedElement.ConductingEquipment'],
                operator=ae['AssessedElement.AssessedSystemOperator'],
                thresholds=[models.Threshold()],
                optimized=secured,
                monitored=scanned,
            )

            # Include CNEC in preventive state if defined
            if in_base_case:
                cnec_preventive = cnec.model_copy(
                    update={"instant": "preventive", "id": f"{ae['IdentifiedObject.mRID']}-preventive"}
                )
                self._crac.flowCnecs.append(cnec_preventive)
                logger.debug(f"Added CNEC {ae['IdentifiedObject.name']} for preventive state")
            else:
                logger.warning(f"Assessed element excluded from preventive state due to 'inBaseCase' is false: {ae['IdentifiedObject.name']}")

            # Include curative CNEC for each contingency defined
            for contingency in self._crac.contingencies:
                cnec_curative = cnec.model_copy(
                    update={"contingencyId": contingency.id, "instant": "curative", "id": f"{ae['IdentifiedObject.mRID']}-curative"}
                )
                self._crac.flowCnecs.append(cnec_curative)
                logger.debug(f"Added CNEC {ae['IdentifiedObject.name']} for curative state on contingency: {contingency.name}")

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

                # TODO [TEMPORARY] - perform consistency check of action (not optimal doing one by one)
                if element_id not in self.network.ID.values:
                    logger.warning(f"Alteration equipment of remedial action does not exist in network model: {action['IdentifiedObject.name_GridStateAlteration']}")
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

    @performance_counter(units='seconds')
    def build_crac(self, contingency_ids: list | None = None):

        # Initialize CRAC object
        self._crac = models.Crac()  # TODO can be replaced with separate function also need to include some general parameters

        # Process contingencies, CNECs and remedial actions
        self.process_contingencies(specific_contingencies=contingency_ids)
        self.process_cnecs()
        self.process_remedial_actions()
        self.update_limits_from_network()

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
