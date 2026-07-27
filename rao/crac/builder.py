from math import isnan
import re
from loguru import logger
import pandas as pd
import triplets
from rao.crac import models
import json
from common.decorators import performance_counter
from rao.crac.context import CracWorkaroundContext


class CracBuilder:
    """
    PreProcessor class for handling pre-processing tasks.
    This class is a placeholder and can be extended with specific pre-processing methods.
    """

    def __init__(self, data: pd.DataFrame, network: pd.DataFrame | None, workaround: CracWorkaroundContext | None = None):
        logger.info(f"CRAC builder initialized")
        self.data = data
        self.network = network
        self.limits = None
        self._crac = None
        self.workaround = workaround or CracWorkaroundContext()

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

    def apply_workarounds(self):
        if not self.workaround.has_3w_replacement():
            return
        logger.info("[WORKAROUND] Applying 3w transformer replacement workaround to CRAC file")

    """
    3W -> 3x2W transformer workaround helpers
    """
    @staticmethod
    def _normalize_grid_element_id(eid: str | None) -> str | None:
        """Normalize grid element ID to avoid double leading underscores.

        The source data (especially contingencies) may contain IDs with two leading
        underscores ("__..."). CRAC typically uses single-underscore IDs ("_...").

        - If the ID starts with one or more underscores, collapse them to a single one.
        - If the ID does not start with an underscore, keep it unchanged.
        """
        if eid is None:
            return None
        s = str(eid)
        if s.startswith("_"):
            return s.lstrip("_")
        return s

    def _infer_hv_mv_nominal_kv(self, base_equipment_id: str) -> tuple[float, float]:
        """Infer HV and MV nominal voltages (kV) for a 3W autotransformer.

        We only have the original 3W transformer in the binary object model, so we
        approximate nominal voltages from SvVoltage values present in `self.limits`.

        Returns:
            (hv_kv, mv_kv)

        Fallbacks:
            - hv_kv defaults to 330 kV
            - mv_kv defaults to 115 kV
        """
        default_hv = 330.0
        default_mv = 115.0

        try:
            if self.limits is None or "SvVoltage.v" not in self.limits.columns or "ID_Equipment" not in self.limits.columns:
                return default_hv, default_mv

            base_id = str(base_equipment_id).lstrip("_")
            vs = self.limits.loc[self.limits["ID_Equipment"] == base_id, "SvVoltage.v"]
            if vs is None or vs.empty:
                return default_hv, default_mv

            vals = pd.to_numeric(vs, errors="coerce").dropna()
            if vals.empty:
                return default_hv, default_mv

            # SvVoltage.v is assumed to be in kV (see MW approximation in get_limits())
            rounded = vals.round(0)
            levels = sorted({float(v) for v in rounded.tolist() if v is not None and float(v) > 1.0})
            if not levels:
                return default_hv, default_mv

            hv = max(levels) if levels else default_hv
            # For 3W, the MV is typically the 2nd highest voltage level.
            mv = levels[-2] if len(levels) >= 2 else default_mv

            # Sanity checks
            if hv <= 0:
                hv = default_hv
            if mv <= 0 or mv >= hv:
                # If we cannot reliably infer, fall back to default MV
                mv = default_mv

            return hv, mv
        except Exception:
            return default_hv, default_mv

    def _get_base_to_legs_map(self, include_leg3: bool = True) -> dict[str, list[str]]:
        """Build mapping from replaced 3W transformer base ID to its 2W leg equipment IDs.

        The mapping is derived from workaround.replaced_3w_trafos index, which contains the
        leg equipment IDs with suffix '-LegX'. The base ID is matched by stripping any leading
        underscores and removing the '-Leg...' suffix.

        Args:
            include_leg3: if False, returns only Leg1 and Leg2 (Leg3 is LV side and not monitored).
        """
        if not self.workaround or not self.workaround.has_3w_replacement():
            return {}

        replaced_3w_trafos = getattr(self.workaround, "replaced_3w_trafos", None)
        if replaced_3w_trafos is None or getattr(replaced_3w_trafos, "empty", True):
            return {}

        df = replaced_3w_trafos.copy()
        # Ensure index is string-like
        df.index = df.index.astype(str)

        if not include_leg3:
            df = df[df.index.str.contains(r"-Leg[12]$", case=False, regex=True)]
            if df.empty:
                return {}

        df = df.assign(
            base_id=lambda x: (x.index.to_series().str.split("-Leg", n=1).str[0]).str.lstrip("_")
        )

        return (
            df.groupby("base_id")
            .apply(lambda g: g.index.tolist())
            .to_dict()
        )

    def get_limits_for_replaced_3w_trafos(self, limits: dict, kind: str | None = None) -> dict:
        """Extend a limits dictionary with entries for replaced 3W transformer legs (Leg1 & Leg2).

        Limits in the binary object model are only available on the original 3W transformer equipment ID
        (base ID, no '-Leg' suffix). When the 3W is replaced with 3x2W, FlowCNECs are created on the legs.
        This function copies the already-retrieved base limit value to the corresponding Leg1/Leg2 IDs.

        Args:
            limits: dict keyed by equipment ID (grid element ID) -> limit value.
            kind: Optional hint about what the limit represents.
                - "current": values are in ampere; Leg2 is scaled by HV/MV nominal voltage ratio.
                - any other value / None: no scaling is applied.

        Notes:
          - Leg3 is intentionally NOT mapped (LV side).
          - Leading underscore variants are added (no underscore, single '_', double '__') to be robust.
          - Existing non-missing leg limits are not overwritten.
        """

        def _is_missing(v) -> bool:
            if v is None:
                return True
            try:
                return bool(pd.isna(v))
            except Exception:
                return False

        def _variants(eid: str) -> list[str]:
            base = str(eid).lstrip("_")
            return [base, "_" + base, "__" + base]

        leg_re = re.compile(r"-Leg(?P<num>[0-9]+)$", re.IGNORECASE)

        def _leg_num(leg_id: str) -> int | None:
            m = leg_re.search(str(leg_id))
            if not m:
                return None
            try:
                return int(m.group("num"))
            except Exception:
                return None

        if not limits:
            return limits

        base_to_legs = self._get_base_to_legs_map(include_leg3=False)
        if not base_to_legs:
            return limits

        # Work on a copy to avoid side-effects surprises
        out = dict(limits)

        mapped = 0
        for base_id, legs in base_to_legs.items():
            # Find base value under any underscore-variant key
            base_value = None
            for key in _variants(base_id):
                v = out.get(key)
                if not _is_missing(v):
                    base_value = v
                    break

            if _is_missing(base_value):
                continue

            # Ensure base aliases exist as well
            for key in _variants(base_id):
                if _is_missing(out.get(key)):
                    out[key] = base_value

            # If this is a current limit, scale Leg2 by HV/MV ratio.
            # Base current limit is assumed to be on the HV side (Leg1).
            scale_leg2 = 1.0
            if kind == "current":
                hv_kv, mv_kv = self._infer_hv_mv_nominal_kv(base_id)
                if mv_kv and mv_kv > 0:
                    scale_leg2 = hv_kv / mv_kv

            # Map base value to Leg1 & Leg2 (with underscore variants)
            for leg_id in legs:
                leg_clean = str(leg_id).lstrip("_")
                leg_num = _leg_num(leg_clean)

                leg_value = base_value
                if kind == "current" and leg_num == 2:
                    # Only scale if we have a numeric value
                    try:
                        leg_value = float(base_value) * float(scale_leg2)
                    except Exception:
                        leg_value = base_value

                for key in _variants(leg_clean):
                    existing = out.get(key)
                    if _is_missing(existing):
                        out[key] = leg_value
                        mapped += 1
                    elif kind == "current" and leg_num == 2:
                        # If earlier logic copied Leg1 current limit to Leg2 unscaled, correct it.
                        # We only overwrite if the existing value equals the base HV value.
                        try:
                            if float(existing) == float(base_value) and float(existing) != float(leg_value):
                                out[key] = leg_value
                                mapped += 1
                        except Exception:
                            pass

        if mapped:
            logger.info(f"[WORKAROUND] Mapped {mapped} limit entries from replaced 3W transformers to 2W legs (Leg1/Leg2)")

        return out

    def flowcnecs_3w_workaround(self):
        """Replace FlowCNECs on replaced 3W transformers with FlowCNECs on their 2W legs.

        - Replaces a FlowCNEC whose networkElementId matches a replaced 3W base ID
          with two FlowCNECs: one for Leg1 and one for Leg2.
        - Leg3 is omitted from FlowCNECs entirely.
        - New IDs follow pattern: '<originalIdBase>-leg{n}-preventive/curative'.
        """
        if not self.workaround or not self.workaround.has_3w_replacement():
            return

        base_to_legs = self._get_base_to_legs_map(include_leg3=False)
        if not base_to_legs:
            return

        flow_cnecs = list(getattr(self._crac, "flowCnecs", []) or [])
        if not flow_cnecs:
            return

        leg_re = re.compile(r"-Leg(?P<num>[0-9]+)$", re.IGNORECASE)

        def _extract_leg_num(leg_id: str, fallback: int) -> int:
            m = leg_re.search(str(leg_id))
            if not m:
                return fallback
            try:
                return int(m.group("num"))
            except Exception:
                return fallback

        def _inject_leg_into_id(flowcnec_id: str, leg_num: int) -> str:
            if flowcnec_id.endswith("-preventive"):
                base = flowcnec_id[: -len("-preventive")]
                return f"{base}-leg{leg_num}-preventive"
            if flowcnec_id.endswith("-curative"):
                base = flowcnec_id[: -len("-curative")]
                return f"{base}-leg{leg_num}-curative"
            return f"{flowcnec_id}-leg{leg_num}"

        new_flow_cnecs = []
        replaced_count = 0
        created_count = 0

        for cnec in flow_cnecs:
            elem_id = getattr(cnec, "networkElementId", "") or ""
            elem_norm = str(elem_id).lstrip("_")

            # Drop any Leg3 CNECs related to replaced 3W transformers (LV side not monitored)
            if "-Leg" in elem_norm:
                base_norm = elem_norm.split("-Leg", 1)[0]
                if base_norm in base_to_legs and elem_norm.lower().endswith("-leg3"):
                    continue

            if elem_norm not in base_to_legs:
                new_flow_cnecs.append(cnec)
                continue

            # Replace base 3W FlowCNEC with Leg1 & Leg2 FlowCNECs
            replaced_count += 1
            legs = base_to_legs[elem_norm]
            for idx, leg_id in enumerate(legs, start=1):
                leg_num = _extract_leg_num(leg_id, fallback=idx)
                # Normalize to a single leading underscore if the source contains underscores
                leg_network_id = self._normalize_grid_element_id(str(leg_id))

                updates = {
                    "networkElementId": leg_network_id,
                    "id": _inject_leg_into_id(getattr(cnec, "id", ""), leg_num=leg_num),
                }
                new_flow_cnecs.append(cnec.model_copy(update=updates))
                created_count += 1

        setattr(self._crac, "flowCnecs", new_flow_cnecs)

        if replaced_count:
            logger.info(
                f"[WORKAROUND] FlowCNECs: replaced {replaced_count} 3W-monitored FlowCNECs "
                f"with {created_count} leg-specific FlowCNECs (Leg1/Leg2 only)"
            )

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

        def _get_limit_fallback_to_patl(instance: object, primary: dict, fallback: dict):
            value = primary.get(instance.networkElementId)
            if value is None:
                fallback_value = fallback.get(instance.networkElementId)
                if fallback_value is not None:
                    logger.warning(f"TATL limit is missing for {instance.name}, using PATL value instead")
                    return fallback_value
            return value

        if self.limits is None:
            self.get_limits()

        logger.info(f"Updating operational limits on CNECs from network model")

        patl_limits = self.limits[self.limits["OperationalLimitType.limitType"].str.endswith(".patl")].groupby("ID_Equipment")
        tatl_limits = self.limits[self.limits["OperationalLimitType.limitType"].str.endswith(".tatl")].groupby("ID_Equipment")

        # Generate mean and max voltages for equipment
        # voltages = patl_limits["SvVoltage.v"].mean().round(1).to_dict()
        # max_voltage = patl_limits["SvVoltage.v"].max().round(1).to_dict()

        patl_current_limits = {}
        tatl_current_limits = {}
        if "CurrentLimit.value" in self.limits.columns:
            patl_current_limits = patl_limits["CurrentLimit.value"].min().to_dict()
            tatl_current_limits = tatl_limits["CurrentLimit.value"].min().to_dict()

        patl_active_power_limits = {}
        tatl_active_power_limits = {}
        if "ActivePowerLimit.value" in self.limits.columns:
            patl_active_power_limits = patl_limits["ActivePowerLimit.value"].min().to_dict()
            tatl_active_power_limits = tatl_limits["ActivePowerLimit.value"].min().to_dict()

        patl_apparent_power_limits = {}
        tatl_apparent_power_limits = {}
        if "ApparentPowerLimit.value" in self.limits.columns:
            patl_apparent_power_limits = patl_limits["ApparentPowerLimit.value"].min().to_dict()
            tatl_apparent_power_limits = tatl_limits["ApparentPowerLimit.value"].min().to_dict()

        # 3W -> 3x2W workaround: map base 3W trafo limits to Leg1 & Leg2 IDs
        if self.workaround and self.workaround.has_3w_replacement():
            patl_current_limits = self.get_limits_for_replaced_3w_trafos(patl_current_limits, kind="current")
            tatl_current_limits = self.get_limits_for_replaced_3w_trafos(tatl_current_limits, kind="current")
            patl_active_power_limits = self.get_limits_for_replaced_3w_trafos(patl_active_power_limits)
            tatl_active_power_limits = self.get_limits_for_replaced_3w_trafos(tatl_active_power_limits)
            patl_apparent_power_limits = self.get_limits_for_replaced_3w_trafos(patl_apparent_power_limits)
            tatl_apparent_power_limits = self.get_limits_for_replaced_3w_trafos(tatl_apparent_power_limits)

        for monitored_element in self._crac.flowCnecs:

            # TODO figure out optimization that same CNEC on preventive and curative instance would be updated

            # Set nominal voltage to operational voltages, taken from SV
            # if operational_voltage := voltages.get(monitored_element.networkElementId):
            #     # TODO add both sides of the equipment when building CRAC nominal voltages
            #     if "_AT" in monitored_element.name:
            #         max_op_voltage = max_voltage.get(monitored_element.networkElementId)
            #         monitored_element.nominalV = [max_op_voltage]
            #         logger.debug(f"Flow CNEC {monitored_element.name} [{monitored_element.instant}] max operational voltage selected: {max_op_voltage}")
            #     else:
            #         monitored_element.nominalV = [operational_voltage]
            #         logger.debug(f"Flow CNEC {monitored_element.name} [{monitored_element.instant}] nominal voltage updated: {operational_voltage}")
            # else:
            #     logger.warning(f"Flow CNEC {monitored_element.name} operational voltage not available, using nominal")

            # Select limits by instant of CNEC
            if monitored_element.instant == "preventive":
                current_limits = patl_current_limits
                active_power_limits = patl_active_power_limits
                apparent_power_limits = patl_apparent_power_limits
            else:
                current_limits = tatl_current_limits
                active_power_limits = tatl_active_power_limits
                apparent_power_limits = tatl_apparent_power_limits

            # Get actual limits
            if limit := _get_limit_fallback_to_patl(instance=monitored_element,
                                                    primary=current_limits,
                                                    fallback=patl_current_limits):
                unit = "ampere"
            elif limit := _get_limit_fallback_to_patl(instance=monitored_element,
                                                      primary=active_power_limits,
                                                      fallback=patl_active_power_limits):
                unit = "megawatt"
            elif limit := _get_limit_fallback_to_patl(instance=monitored_element,
                                                      primary=apparent_power_limits,
                                                      fallback=patl_apparent_power_limits):
                unit = "apparent"
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

    def contingencies_3w_workaround(self):
        """
        If replace 3w trafo with 3x 2w trafo workaround is enabled, re-build contingencies to ensure that all the replaced 3w transformer legs are part of the contingency element(s)
        Returns: Replaced contingency (if a 3w trafo contingency was passed) with correct one that has all the replaced 3w trafo legs grid element IDs included
        """
        if not self.workaround.has_3w_replacement():
            return

        # Map: base_3w_id -> list of 2w leg ids (include Leg3 for contingencies)
        base_to_legs = self._get_base_to_legs_map(include_leg3=True)

        for contingency in self._crac.contingencies:

            original_ids = contingency.networkElementsIds or [ ]
            if not original_ids:
                continue

            new_ids: list[ str ] = [ ]
            replaced = False

            for elem_id in original_ids:
                elem_str = str(elem_id)
                elem_norm = elem_str.lstrip("_")

                # If the contingency already references a leg id ('...-LegX'), use its base id
                base_norm = elem_norm.split("-Leg", 1)[ 0 ]

                if base_norm in base_to_legs:
                    # Replace the 3W element (or any single leg) with *all* its 2W legs
                    new_ids.extend(base_to_legs[ base_norm ])
                    replaced = True
                else:
                    new_ids.append(elem_str)

            # Always normalize leading underscores (fix '__' -> '_') and deduplicate while preserving order
            new_ids = [ self._normalize_grid_element_id(x) for x in new_ids if x ]
            contingency.networkElementsIds = list(dict.fromkeys(new_ids))

            if replaced:
                logger.info(f"[WORKAROUND] Contingency {contingency.name} 3W elements replaced with 2W elements")

    def process_cnecs(self):
        """
        We want to always monitor all assessed elements, so we create CNECs for each assessed element.
        This process always looks in already defined contingencies to have in synchronized.
        """

        assessed_elements = self.data.type_tableview("AssessedElement", string_to_number=False)

        # Assessed elements defined through an OperationalLimit reference (VoltageLimit / VoltageAngleLimit)
        # are voltage/angle monitoring CNECs -> handled separately in process_monitoring_cnecs()
        if 'AssessedElement.ConductingEquipment' not in assessed_elements.columns:
            assessed_elements['AssessedElement.ConductingEquipment'] = pd.NA
        if 'AssessedElement.OperationalLimit' in assessed_elements.columns:
            limit_based = (assessed_elements['AssessedElement.ConductingEquipment'].isna()
                           & assessed_elements['AssessedElement.OperationalLimit'].notna())
            assessed_elements = assessed_elements[~limit_based]

        # TODO [TEMPORARY] - perform consistency check
        missing = assessed_elements[~assessed_elements['AssessedElement.ConductingEquipment'].isin(self.network.ID)]
        for _, row in missing.iterrows():
            logger.warning(f"Assessed element does not exist in network model: {row['IdentifiedObject.name']}")
        assessed_elements = assessed_elements.drop(index=missing.index)

        # TODO [TEMPORARY] - pre-emptive exclusion of 3W transformers from CNECs, disabled due to 3W workaround.
        # _power_transformers = self.network.type_tableview('PowerTransformer')
        # _transformer_ends = self.network.type_tableview('PowerTransformerEnd')
        # _power_transformers = _power_transformers.merge(
        #     _transformer_ends['PowerTransformerEnd.PowerTransformer'].value_counts(),
        #     left_index=True,
        #     right_index=True,
        #     how='left'
        # )
        # transformers_3w = _power_transformers[_power_transformers['count'] == 3]
        # exclude = assessed_elements[assessed_elements['AssessedElement.ConductingEquipment'].isin(transformers_3w.index)]
        # for _, row in exclude.iterrows():
        #     logger.warning(f"Assessed element excluded as 3W transformers not supported: {row['IdentifiedObject.name']}")
        # assessed_elements = assessed_elements.drop(index=exclude.index)

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

    """
    Voltage / Angle monitoring CNEC helpers (OpenRAO NC CRAC format)
    """
    def _get_object_attributes(self, object_id: str | None) -> dict:
        """Collect KEY -> VALUE attributes for a given object ID across the NC profile
        data and the network (EQ/CGMES) triplet stores.

        VoltageAngleLimits (and their limit sets/types) come with the ER profile in `self.data`,
        while VoltageLimits are defined in the EQ profile in `self.network`, so both stores are checked.
        """
        attributes = {}
        if not object_id:
            return attributes
        for store in (self.data, self.network):
            if store is None:
                continue
            rows = store[store.ID == object_id]
            for key, value in zip(rows.KEY, rows.VALUE):
                attributes.setdefault(key, value)
        return attributes

    def _resolve_voltage_level(self, equipment_id: str | None) -> str | None:
        """Resolve the VoltageLevel that contains a given piece of equipment (e.g. a BusBarSection).

        OpenRAO VoltageCnecs must reference a VoltageLevel of the network model, while the NC profile
        VoltageLimit's OperationalLimitSet references a BusBarSection. Traverse the equipment
        containment hierarchy (Equipment.EquipmentContainer / Bay.VoltageLevel) up to the VoltageLevel.
        """
        current = equipment_id
        for _ in range(3):  # BusBarSection -> (Bay) -> VoltageLevel
            if not current:
                return None
            attributes = self._get_object_attributes(current)
            if attributes.get('Type') == 'VoltageLevel':
                return current
            current = attributes.get('Equipment.EquipmentContainer') or attributes.get('Bay.VoltageLevel')
        return None

    def _resolve_terminal_equipment(self, terminal_id: str | None) -> str | None:
        """Resolve the conducting equipment referenced by a terminal"""
        return self._get_object_attributes(terminal_id).get('Terminal.ConductingEquipment')

    def _append_monitoring_cnec(self, cnec, ae: dict):
        """Append a monitoring CNEC (voltage or angle) to the CRAC for the preventive state
        (if inBaseCase) and for the curative state of each defined contingency,
        mirroring the FlowCNEC processing logic."""
        target = self._crac.voltageCnecs if isinstance(cnec, models.VoltageCnec) else self._crac.angleCnecs
        cnec_kind = type(cnec).__name__

        in_base_case = str(ae.get("AssessedElement.inBaseCase", "false")).lower() == 'true'
        if in_base_case:
            target.append(cnec.model_copy(update={"instant": "preventive", "id": f"{cnec.id}-preventive"}))
            logger.debug(f"Added {cnec_kind} {cnec.name} for preventive state")
        else:
            logger.warning(f"Assessed element excluded from preventive state due to 'inBaseCase' is false: {cnec.name}")

        for contingency in self._crac.contingencies:
            target.append(cnec.model_copy(
                update={"contingencyId": contingency.id, "instant": "curative", "id": f"{cnec.id}-curative"}
            ))
            logger.debug(f"Added {cnec_kind} {cnec.name} for curative state on contingency: {contingency.name}")

    def _create_voltage_cnec(self, ae: dict, limit_attributes: dict):
        """Create a VoltageCnec from an assessed element referencing a VoltageLimit.

        Per OpenRAO NC CRAC format:
            - network element is the VoltageLevel of the BusBarSection referenced by
              the limit's OperationalLimitSet.Equipment
            - threshold unit is kilovolt; limitType 'highVoltage' -> max, 'lowVoltage' -> min
            - only permanent limits (isInfiniteDuration=true, the default) are supported
        """
        name = ae['IdentifiedObject.name']

        limit_type_attributes = self._get_object_attributes(limit_attributes.get('OperationalLimit.OperationalLimitType'))
        limit_set_attributes = self._get_object_attributes(limit_attributes.get('OperationalLimit.OperationalLimitSet'))

        # Only permanent voltage limits (with infinite duration) are handled by OpenRAO
        if str(limit_type_attributes.get('OperationalLimitType.isInfiniteDuration', 'true')).lower() == 'false':
            logger.warning(f"VoltageCnec excluded, only permanent voltage limits (infinite duration) are supported: {name}")
            return

        try:
            value = float(limit_attributes.get('VoltageLimit.value'))
        except (TypeError, ValueError):
            logger.warning(f"VoltageCnec excluded due to missing or invalid VoltageLimit value: {name}")
            return

        limit_kind = str(limit_type_attributes.get('OperationalLimitType.limitType', ''))
        if limit_kind.endswith('highVoltage'):
            threshold = models.MonitoringThreshold(unit='kilovolt', max=value)
        elif limit_kind.endswith('lowVoltage'):
            threshold = models.MonitoringThreshold(unit='kilovolt', min=value)
        else:
            logger.warning(f"VoltageCnec excluded, voltage limit can only be of kind highVoltage or lowVoltage: {name}")
            return

        # Resolve BusBarSection referenced by the limit set to its VoltageLevel
        equipment_id = (limit_set_attributes.get('OperationalLimitSet.Equipment')
                        or self._resolve_terminal_equipment(limit_set_attributes.get('OperationalLimitSet.Terminal')))
        voltage_level_id = self._resolve_voltage_level(equipment_id)
        if not voltage_level_id:
            logger.warning(f"VoltageCnec excluded, could not resolve VoltageLevel for equipment {equipment_id}: {name}")
            return

        cnec = models.VoltageCnec(
            id=f"{ae['IdentifiedObject.mRID']}",
            name=name,
            description=ae.get('IdentifiedObject.description') or "",
            networkElementId=voltage_level_id,
            operator=ae['AssessedElement.AssessedSystemOperator'],
            thresholds=[threshold],
            monitored=True,
            optimized=False,  # VoltageCnecs cannot be optimized by the RAO, only monitored
        )
        self._append_monitoring_cnec(cnec, ae)

    def _create_angle_cnec(self, ae: dict, limit_attributes: dict):
        """Create an AngleCnec from an assessed element referencing a VoltageAngleLimit (ER profile).

        Per OpenRAO NC CRAC format:
            - terminal_1 = VoltageAngleLimit.AngleReferenceTerminal, terminal_2 = OperationalLimitSet.Terminal
            - isFlowToRefTerminal true -> exporting=terminal_1, importing=terminal_2 (false/missing -> reversed)
            - threshold unit is degree; direction 'high' -> max=+value, 'low' -> min=-value,
              'absoluteValue' -> min=-value and max=+value
            - if direction is not 'absoluteValue', isFlowToRefTerminal must be present, otherwise ignored
        """
        name = ae['IdentifiedObject.name']

        limit_type_attributes = self._get_object_attributes(limit_attributes.get('OperationalLimit.OperationalLimitType'))
        limit_set_attributes = self._get_object_attributes(limit_attributes.get('OperationalLimit.OperationalLimitSet'))

        try:
            value = float(limit_attributes.get('VoltageAngleLimit.normalValue'))
        except (TypeError, ValueError):
            logger.warning(f"AngleCnec excluded due to missing or invalid VoltageAngleLimit normalValue: {name}")
            return
        if value < 0:
            logger.warning(f"AngleCnec excluded, VoltageAngleLimit normalValue must be positive: {name}")
            return

        direction = str(limit_type_attributes.get('OperationalLimitType.direction', ''))
        flow_to_ref = limit_attributes.get('VoltageAngleLimit.isFlowToRefTerminal')
        if direction.endswith('absoluteValue'):
            threshold = models.MonitoringThreshold(unit='degree', min=-value, max=value)
        elif direction.endswith('.high'):
            threshold = models.MonitoringThreshold(unit='degree', max=value)
        elif direction.endswith('.low'):
            threshold = models.MonitoringThreshold(unit='degree', min=-value)
        else:
            logger.warning(f"AngleCnec excluded due to unsupported OperationalLimitType direction '{direction}': {name}")
            return
        if not direction.endswith('absoluteValue') and flow_to_ref is None:
            logger.warning(f"AngleCnec excluded, 'isFlowToRefTerminal' must be defined when direction is not absoluteValue: {name}")
            return

        # Resolve the two terminals into network elements (importing / exporting ends)
        terminal_1 = self._resolve_terminal_equipment(limit_attributes.get('VoltageAngleLimit.AngleReferenceTerminal'))
        terminal_2 = self._resolve_terminal_equipment(limit_set_attributes.get('OperationalLimitSet.Terminal'))
        if not terminal_1 or not terminal_2:
            logger.warning(f"AngleCnec excluded, could not resolve terminal network elements: {name}")
            return

        if str(flow_to_ref).lower() == 'true':
            exporting_element, importing_element = terminal_1, terminal_2
        else:
            exporting_element, importing_element = terminal_2, terminal_1

        cnec = models.AngleCnec(
            id=f"{ae['IdentifiedObject.mRID']}",
            name=name,
            description=ae.get('IdentifiedObject.description') or "",
            exportingNetworkElementId=exporting_element,
            importingNetworkElementId=importing_element,
            operator=ae['AssessedElement.AssessedSystemOperator'],
            thresholds=[threshold],
            monitored=True,
            optimized=False,  # AngleCnecs cannot be optimized by the RAO, only monitored
        )
        self._append_monitoring_cnec(cnec, ae)

    def process_monitoring_cnecs(self):
        """
        Create Voltage CNECs and Angle CNECs from assessed elements defined through an
        OperationalLimit reference, per the OpenRAO NC CRAC format:
            - AssessedElement.OperationalLimit -> VoltageLimit      => VoltageCnec
            - AssessedElement.OperationalLimit -> VoltageAngleLimit => AngleCnec
        All assessed elements (incl. Baltic CCR XNEs) present in the AE profile are processed;
        the same normalEnabled / inBaseCase / contingency synchronization rules apply as for FlowCNECs.
        """
        assessed_elements = self.data.type_tableview("AssessedElement", string_to_number=False)

        if 'AssessedElement.OperationalLimit' not in assessed_elements.columns:
            logger.info("No limit-based assessed elements found, skipping voltage/angle CNEC creation")
            return

        limit_based = assessed_elements[assessed_elements['AssessedElement.OperationalLimit'].notna()]
        logger.info(f"Processing {len(limit_based)} limit-based assessed elements for voltage/angle CNECs")

        for ae in limit_based.to_dict('records'):

            # Exclude assessed elements which normalEnabled = false
            if str(ae.get('AssessedElement.normalEnabled', 'false')).lower() == 'false':
                logger.warning(f"Assessed element excluded due to 'normalEnabled' is false or missing: {ae['IdentifiedObject.name']}")
                continue

            limit_attributes = self._get_object_attributes(ae['AssessedElement.OperationalLimit'])
            limit_type = limit_attributes.get('Type')

            if limit_type == 'VoltageLimit':
                self._create_voltage_cnec(ae, limit_attributes)
            elif limit_type == 'VoltageAngleLimit':
                self._create_angle_cnec(ae, limit_attributes)
            else:
                logger.warning(f"Assessed element excluded, unsupported OperationalLimit type '{limit_type}': {ae['IdentifiedObject.name']}")

    def process_remedial_actions(self):
        """
        TopologyAction type grid alteration can only have property range with direction "none" or "upAndDown"
        "none" - sets value only what defined under normalValue
        "upAndDown" - allows RAO to connect or disconnect equipment not depending on what in normalValue or what
        actual status of equipment in current case
        """

        def _get_opposite_terminal_connection_value(value: str):
            return "open" if value == "close" else "close"

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

            # Get relevant property ranges for the remedial action
            ranges = property_ranges[property_ranges['RangeConstraint.GridStateAlteration'].isin(data['IdentifiedObject.mRID_GridStateAlteration'])]
            # Validate whether all alterations similar direction attribute of property ranges
            directions = ranges['RangeConstraint.direction'].apply(lambda x: x.split(".")[-1])
            if len(directions.unique()) > 1:
                logger.warning(f"Remedial action contains alterations with different property range directions: {mrid}")
                logger.warning(f"Not supported by CRAC builder, ignoring remedial action")
                continue

            # Create network elements property modification
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

            # Check whether directions is "upAndDown", then multiple network actions have to be defined in CRAC
            if directions.unique().item() == "upAndDown" and getattr(network_action, "terminalsConnectionActions", None):
                logger.debug(f"Remedial action defined with 'upAndDown' direction, adding opposite terminal connection actions")
                _opposite_terminal_actions = [x.model_copy(update={"actionType": _get_opposite_terminal_connection_value(x.actionType)}) for x in actions]
                _updates = {
                    "id": f"{data['IdentifiedObject.mRID_GridStateAlterationRemedialAction'].iloc[0]}-opposite-direction",
                    "terminalsConnectionActions": _opposite_terminal_actions,
                }
                opposite_network_action = network_action.model_copy(update=_updates)
                self._crac.networkActions.append(opposite_network_action)

    def _get_replaced_3w_grid_state_alteration_ids(self) -> dict[str, list[str]]:
        """Map replaced 3W transformer IDs to CRAC grid-state alteration IDs.

        The original CRAC remedial action data references the 3W transformer as
        topology action equipment. After the network workaround replaces that
        transformer with 2W legs, this mapping identifies which grid-state
        alterations originally targeted each replaced 3W transformer.
        """
        if not self.workaround or not self.workaround.has_3w_replacement():
            return {}

        base_to_legs = self._get_base_to_legs_map(include_leg3=True)
        if not base_to_legs:
            return {}

        try:
            topology_actions = self.data.type_tableview("TopologyAction", string_to_number=False)
        except Exception:
            return {}

        if topology_actions is None or topology_actions.empty:
            return {}

        equipment_column = None
        for candidate in "TopologyAction.Equipment":
            if "TopologyAction.Equipment" in topology_actions.columns:
                equipment_column = candidate
                break

        if equipment_column is None or "IdentifiedObject.mRID" not in topology_actions.columns:
            return {}

        base_ids = set(base_to_legs)
        result: dict[str, list[str]] = {base_id: [] for base_id in base_ids}

        for row in topology_actions.to_dict("records"):
            equipment_id = row.get(equipment_column)
            if equipment_id is None:
                continue

            equipment_norm = str(equipment_id).lstrip("_")
            base_norm = equipment_norm.split("-Leg", 1)[0]
            if base_norm not in base_ids:
                continue

            alteration_id = row.get("IdentifiedObject.mRID")
            if alteration_id and alteration_id not in result[base_norm]:
                result[base_norm].append(alteration_id)

        return {base_id: alteration_ids for base_id, alteration_ids in result.items() if alteration_ids}

    def remedial_actions_3w_workaround(self):
        """
        If replace 3w trafo with 3x 2w trafo workaround is enabled, re-build remedial actions
        to ensure terminal connection actions targeting replaced 3w transformers target all
        replacement 2w transformer legs instead.
        """
        if not self.workaround or not self.workaround.has_3w_replacement():
            return

        base_to_legs = self._get_base_to_legs_map(include_leg3=True)
        if not base_to_legs:
            return

        network_actions = list(getattr(self._crac, "networkActions", []) or [])
        if not network_actions:
            return

        leg_re = re.compile(r"-Leg(?P<num>[0-9]+)$", re.IGNORECASE)

        def _leg_sort_key(leg_id: str) -> tuple[int, str]:
            m = leg_re.search(str(leg_id))
            if not m:
                return (999, str(leg_id))
            try:
                return (int(m.group("num")), str(leg_id))
            except Exception:
                return (999, str(leg_id))

        alteration_ids_by_3w = self._get_replaced_3w_grid_state_alteration_ids()
        if alteration_ids_by_3w:
            logger.debug(
                f"[WORKAROUND] Found remedial action grid-state alterations for replaced 3W transformers: {alteration_ids_by_3w}")

        replaced_count = 0
        created_count = 0

        for network_action in network_actions:
            terminal_actions = getattr(network_action, "terminalsConnectionActions", None)
            if not terminal_actions:
                continue

            new_terminal_actions: list[models.TerminalsAction] = []
            replaced_in_network_action = False

            for terminal_action in terminal_actions:
                elem_id = getattr(terminal_action, "networkElementId", "") or ""
                elem_norm = str(elem_id).lstrip("_")
                base_norm = elem_norm.split("-Leg", 1)[0]

                if base_norm not in base_to_legs:
                    new_terminal_actions.append(terminal_action.model_copy())
                    continue

                legs = sorted(base_to_legs[base_norm], key=_leg_sort_key)
                if not legs:
                    logger.warning(f"[WORKAROUND] Remedial action {network_action.name} references replaced 3W transformer {elem_id}, but no replacement 2W legs were found")
                    new_terminal_actions.append(terminal_action.model_copy())
                    continue

                replaced_in_network_action = True
                replaced_count += 1
                for leg_id in legs:
                    new_terminal_actions.append(
                        terminal_action.model_copy(
                            update={"networkElementId": self._normalize_grid_element_id(str(leg_id))}
                        )
                    )
                    created_count += 1

            if replaced_in_network_action:
                network_action.terminalsConnectionActions = new_terminal_actions

        if replaced_count:
            logger.info(f"[WORKAROUND] Remedial actions: replaced {replaced_count} 3W terminal connection actions with {created_count} leg-specific terminal connection actions"
            )

    @performance_counter(units='seconds')
    def build_crac(self, contingency_ids: list | None = None):

        # Initialize CRAC object
        self._crac = models.Crac()  # TODO can be replaced with separate function also need to include some general parameters

        # Apply workaround-specific flags to CRAC building process
        self.apply_workarounds()

        # Process contingencies, CNECs and remedial actions
        self.process_contingencies(specific_contingencies=contingency_ids)
        if self.workaround:
            logger.info("[WORKAROUND] Applying 3w transformer replacement workaround to contingencies")
            self.contingencies_3w_workaround()
        self.process_cnecs()
        self.process_monitoring_cnecs()

        if self.workaround:
            logger.info("[WORKAROUND] Applying 3w transformer replacement workaround to FlowCNECs")
            self.flowcnecs_3w_workaround()

        self.process_remedial_actions()
        if self.workaround:
            logger.info("[WORKAROUND] Applying 3w transformer replacement workaround to Remedial actions")
            self.remedial_actions_3w_workaround()

        # Update the CRAC flowCNEC limits from network
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
