import os
from rao.parameters.manager import LoadflowSettingsManager

mgr = LoadflowSettingsManager()
print(mgr.config)
pp_mgr = mgr.build_pypowsybl_parameters()
# Test accessors
print('Sample read:', mgr.get('LF_PARAMETERS.write_slack_bus', None))
# mgr.set('LF_PROVIDER.maxNewtonRaphsonIterations', '25')
# print('After set:', mgr.get('LF_PROVIDER.maxNewtonRaphsonIterations'))

# Test export
print(mgr.to_bytesio('json').getvalue()[:120].decode('utf-8') + '...')

# Test override loading
os.environ['LOADFLOW_CONFIG_OVERRIDE_PATH'] = 'lf_settings_override.json'
override_mgr = LoadflowSettingsManager()
print(override_mgr.config)
pp_override_mgr = override_mgr.build_pypowsybl_parameters()