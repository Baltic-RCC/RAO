import pypowsybl

CGMES_IMPORT_PARAMETERS = {
    "iidm.import.cgmes.source-for-iidm-id": "rdfID",
    "iidm.import.cgmes.import-node-breaker-as-bus-breaker": "True",
}

LF_PROVIDER = {
    # 'slackBusSelectionMode': 'MOST_MESHED',
    # 'slackBusesIds': '',
    # 'lowImpedanceBranchMode': 'REPLACE_BY_ZERO_IMPEDANCE_LINE',
    # 'voltageRemoteControl': 'True',
    # 'slackDistributionFailureBehavior': 'LEAVE_ON_SLACK_BUS',
    'loadPowerFactorConstant': 'False',
    # 'plausibleActivePowerLimit': '5000.0',
    'slackBusPMaxMismatch': '0.1',
    # 'voltagePerReactivePowerControl': 'False',
    # 'generatorReactivePowerRemoteControl': 'False',
    # 'transformerReactivePowerControl': 'False',
    'maxNewtonRaphsonIterations': '15',
    'maxOuterLoopIterations': '30',
    # 'newtonRaphsonConvEpsPerEq': '1.0E-4',
    # 'voltageInitModeOverride': 'VOLTAGE_MAGNITUDE',
    'transformerVoltageControlMode': 'AFTER_GENERATOR_VOLTAGE_CONTROL',  # incremental mode in some cases separates parallel trafos taps
    # 'shuntVoltageControlMode': 'INCREMENTAL_VOLTAGE_CONTROL',
    # 'minPlausibleTargetVoltage': '0.8',
    # 'maxPlausibleTargetVoltage': '1.2',
    # 'minRealisticVoltage': '0.5',
    # 'maxRealisticVoltage': '2.0',
    # 'reactiveRangeCheckMode': 'MAX',
    'lowImpedanceThreshold': '1.0E-5',
    # 'networkCacheEnabled': 'False',
    # 'svcVoltageMonitoring': 'True',
    # 'stateVectorScalingMode': None,
    # 'maxSlackBusCount': '1',
    # 'debugDir': '',
    # 'incrementalTransformerVoltageControlOuterLoopMaxTapShift': '3',
    # 'secondaryVoltageControl': 'False',
    # 'reactiveLimitsMaxPqPvSwitch': '3',
    'newtonRaphsonStoppingCriteriaType': 'PER_EQUATION_TYPE_CRITERIA',
    'maxActivePowerMismatch': '0.1',
    'maxReactivePowerMismatch': '0.1',
    'maxVoltageMismatch': '1.0E-4',
    'maxAngleMismatch': '1.0E-5',
    # 'maxRatioMismatch': '1.0E-5',
    # 'maxSusceptanceMismatch': '1.0E-4',
    # 'phaseShifterControlMode': 'CONTINUOUS_WITH_DISCRETISATION',
    # 'alwaysUpdateNetwork': 'False',
    # 'mostMeshedSlackBusSelectorMaxNominalVoltagePercentile': '95.0',
    # 'reportedFeatures': [],
    'slackBusCountryFilter': 'PL',
    # 'actionableSwitchesIds': [],
    # 'actionableTransformersIds': [],
    # 'asymmetrical': 'False',
    # 'minNominalVoltageTargetVoltageCheck': '20.0',
    # 'reactivePowerDispatchMode': 'Q_EQUAL_PROPORTION',
    # 'outerLoopNames': [],
    # 'useActiveLimits': 'True',
    # 'disableVoltageControlOfGeneratorsOutsideActivePowerLimits': 'False',
    # 'lineSearchStateVectorScalingMaxIteration': '10',
    # 'lineSearchStateVectorScalingStepFold': '1.33',
    # 'maxVoltageChangeStateVectorScalingMaxDv': '0.1',
    # 'maxVoltageChangeStateVectorScalingMaxDphi': '0.1745',
    # 'linePerUnitMode': 'IMPEDANCE',
    # 'useLoadModel': 'False',
    # 'dcApproximationType': 'IGNORE_R',
    # 'simulateAutomationSystems': 'False',
    # 'acSolverType': 'NEWTON_RAPHSON',
    # 'maxNewtonKrylovIterations': '100',
    # 'newtonKrylovLineSearch': 'False',
    # 'referenceBusSelectionMode': 'FIRST_SLACK',  # GENERATOR_REFERENCE_PRIORITY
    # 'writeReferenceTerminals': 'True',
    # 'voltageTargetPriorities': 'GENERATOR,TRANSFORMER,SHUNT',
}

LF_PARAMETERS = pypowsybl.loadflow.Parameters(
    voltage_init_mode=pypowsybl.loadflow.VoltageInitMode.UNIFORM_VALUES,
    transformer_voltage_control_on=False,
    use_reactive_limits=True,
    phase_shifter_regulation_on=False,
    twt_split_shunt_admittance=None,
    shunt_compensator_voltage_control_on=False,
    read_slack_bus=False,
    write_slack_bus=True,
    distributed_slack=True,
    balance_type=pypowsybl.loadflow.BalanceType.PROPORTIONAL_TO_GENERATION_REMAINING_MARGIN,
    dc_use_transformer_ratio=None,
    countries_to_balance=None,
    connected_component_mode=pypowsybl.loadflow.ConnectedComponentMode.ALL,
    dc_power_factor=None,
    provider_parameters=LF_PROVIDER,
)