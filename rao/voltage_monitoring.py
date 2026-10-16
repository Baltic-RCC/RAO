def _elementary_actions(crac_action: dict):
    """Yield the elementary actions of a CRAC network action as flat dictionaries.

    Each elementary action collection carries a different value field, so the
    kind is emitted explicitly instead of being inferred from which key is set.
    """
    for elementary in (crac_action.get('terminalsConnectionActions') or []):
        yield {
            'elementaryType': 'terminalsConnection',
            'networkElementId': elementary.get('networkElementId'),
            'actionType': elementary.get('actionType'),
            'sectionCount': None,
        }
    for elementary in (crac_action.get('shuntCompensatorPositionActions') or []):
        yield {
            'elementaryType': 'shuntCompensatorPosition',
            'networkElementId': elementary.get('networkElementId'),
            'actionType': None,
            'sectionCount': elementary.get('sectionCount'),
        }


def _collapse(values: list) -> Any:
    """Return a lone distinct value as a scalar, otherwise the sorted distinct list.

    Remedial actions almost always apply one kind of elementary action, so a
    scalar keeps the Elastic document readable; mixed actions stay complete.
    """
    distinct = {value for value in values if value is not None}
    if not distinct:
        return None
    if len(distinct) == 1:
        return distinct.pop()
    return sorted(distinct, key=str)


def build_applied_actions(rao_results: dict, crac: dict) -> list[dict]:
    """Expand optimized network actions into structured Elastic documents.

    Emits one entry per (remedial action, activated state), with the affected
    network elements as a list, so each attribute lands in its own Elastic
    field instead of a single opaque string. Elasticsearch flattens arrays of
    objects and drops the pairing between sibling fields, so the correlated
    tuple is also pre-joined into 'label' for terms aggregations.
    """
    crac_actions = {a['id']: a for a in crac.get('networkActions', [])}
    applied_actions = []

    for action_result in rao_results.get('networkActionResults', []):
        action_id = action_result.get('networkActionId')
        crac_action = crac_actions.get(action_id, {})
        # Stays empty when the CRAC carries no details for the action
        elementary_actions = list(_elementary_actions(crac_action))

        action_type = _collapse([e['actionType'] for e in elementary_actions])
        section_count = _collapse([e['sectionCount'] for e in elementary_actions])

        for state in (action_result.get('activatedStates') or [{}]):
            entry = {
                'id': action_id,
                'name': crac_action.get('name'),
                'operator': crac_action.get('operator'),
                'instant': state.get('instant'),
                'contingencyId': state.get('contingency'),
                'elementaryType': _collapse([e['elementaryType'] for e in elementary_actions]),
                'actionType': action_type,
                'sectionCount': section_count,
                'networkElementIds': [e['networkElementId'] for e in elementary_actions
                                      if e['networkElementId'] is not None],
            }
            value = action_type if action_type is not None else section_count
            entry['label'] = " | ".join(
                str(part) for part in (entry['name'] or action_id, entry['instant'], value)
                if part is not None
            )
            applied_actions.append(entry)

    return applied_actions


# Voltage limits are reported as separate documents, matching csa-bus-results
# where v_mag_percent is relative to the limit in scope rather than to nominal_v.
VOLTAGE_LIMIT_TYPES = (
    ('HIGH_VOLTAGE', 'max_voltage', 'high_voltage_limit'),
    ('LOW_VOLTAGE', 'min_voltage', 'low_voltage_limit'),
)


def _finite(value: Any) -> Any:
    """Return None for missing or non-finite numbers, which Elasticsearch rejects."""
    if value is None:
        return None
    try:
        if not np.isfinite(value):
            return None
        return float(value)
    except TypeError:
        return value


def _voltage_level_meta(network) -> dict:
    """Map voltage level id to its nominal voltage, substation name and country."""
    meta = {}
    try:
        voltage_levels = network.get_voltage_levels()
        substations = network.get_substations()
    except Exception as error:
        logger.warning(f"Could not read voltage level metadata from network: {error}")
        return meta

    for element_id, voltage_level in voltage_levels.iterrows():
        substation_id = voltage_level.get('substation_id')
        substation = substations.loc[substation_id] if substation_id in substations.index else {}
        meta[str(element_id)] = {
            'nominal_v': _finite(voltage_level.get('nominal_v')),
            'substation_name': substation.get('name') or None,
            'country': substation.get('country') or None,
        }

    return meta


def _lookup_element(meta: dict, element_id: str | None) -> dict:
    """Resolve a network element, tolerating the CRAC's leading underscore prefix."""
    if not element_id:
        return {}
    return meta.get(element_id) or meta.get(element_id.lstrip('_')) or meta.get(f"_{element_id}") or {}


def build_voltage_documents(voltage_results: pd.DataFrame,
                            crac: dict,
                            network,
                            headers: dict,
                            applied_actions: list[dict]) -> list[dict]:
    """Build post-RAO voltage documents using the csa-bus-results field naming.

    Emits one document per (voltage CNEC, limit type) so that v_mag_percent is
    always relative to the limit in scope, the way csa-bus-results reports the
    N-1 base case. Sharing the field names lets both be charted in one Kibana
    view, separated by the 'state' field.
    """
    element_meta = _voltage_level_meta(network)
    contingency_names = {c['id']: c.get('name') for c in crac.get('contingencies', [])}

    cnec_meta = {}
    for cnec in crac.get('voltageCnecs', []):
        threshold = (cnec.get('thresholds') or [{}])[0]
        cnec_meta[cnec['id']] = {
            'subject_id': cnec.get('networkElementId'),
            'subject_name': cnec.get('name'),
            'operator': cnec.get('operator'),
            'instant': cnec.get('instant'),
            'contingency_id': cnec.get('contingencyId'),
            'low_voltage_limit': _finite(threshold.get('min')),
            'high_voltage_limit': _finite(threshold.get('max')),
            'unit': threshold.get('unit'),
        }

    metadata = {
        'run_id': headers.get('run-id'),
        'source_module': headers.get('source-module'),
        'trigger': headers.get('trigger'),
        'project_name': headers.get('project-name'),
        'content_reference': headers.get('content-reference'),
        'username': headers.get('username'),
    }

    documents = []
    for _, row in voltage_results.iterrows():
        cnec = cnec_meta.get(row.get('cnec_id'), {})
        element = _lookup_element(element_meta, cnec.get('subject_id'))
        contingency_id = cnec.get('contingency_id')

        for limit_type, voltage_column, limit_field in VOLTAGE_LIMIT_TYPES:
            limit = cnec.get(limit_field)
            v_mag = _finite(row.get(voltage_column))
            # Skip the direction when the CRAC defines no threshold for it
            if limit is None or v_mag is None:
                continue

            documents.append({
                'state': 'POST_RAO',
                'subject_id': cnec.get('subject_id'),
                'subject_name': cnec.get('subject_name'),
                'subject_type': 'VOLTAGE_LEVEL',
                'substation_name': element.get('substation_name'),
                'country': element.get('country'),
                'nominal_v': element.get('nominal_v'),
                'v_mag': v_mag,
                'v_mag_percent': (v_mag / limit) * 100 if limit else None,
                'limit_type': limit_type,
                'high_voltage_limit': cnec.get('high_voltage_limit'),
                'low_voltage_limit': cnec.get('low_voltage_limit'),
                'is_violation': bool(v_mag > limit if limit_type == 'HIGH_VOLTAGE' else v_mag < limit),
                # Monitoring reports one margin per CNEC - the tightest constraint in
                # either direction - so derive the margin for the limit in scope.
                'margin': (limit - v_mag) if limit_type == 'HIGH_VOLTAGE' else (v_mag - limit),
                'margin_reported': _finite(row.get('margin')),
                'unit': cnec.get('unit'),
                'contingency_id': contingency_id,
                'contingency_name': contingency_names.get(contingency_id),
                'instant': cnec.get('instant'),
                'operator': cnec.get('operator'),
                'operator_strategy_id': '',
                'cnec_id': row.get('cnec_id'),
                'metadata': metadata,
                '@time_horizon': headers.get('time-horizon'),
                '@scenario_timestamp': headers.get('scenario-time'),
                'raoAppliedActions': applied_actions,
                'raoAppliedActionCount': len(applied_actions),
            })

    return documents