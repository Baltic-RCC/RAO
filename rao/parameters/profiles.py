"""RAO parameter profiles tried in order by the optimizer worker.

The first profile is the default configuration. Each following one is a
fallback applied only when the previous attempt failed. Overrides use the
dotted paths understood by RaoSettingsManager.set().
"""

# Objective function overrides per RAO_OBJECTIVE setting, applied before the profile overrides.
# OpenRAO 7.3.0 (pypowsybl 1.16.1) fails MIN_COST without 'costly-min-margin-parameters', and MIN_COST
# needs 'pst-model' APPROXIMATED_INTEGERS
OBJECTIVE_OVERRIDES = {
    "MAX_MIN_MARGIN": {
        "objective-function.type": "MAX_MIN_MARGIN",
    },
    "MIN_COST": {
        "objective-function.type": "MIN_COST",
        "extensions.open-rao-search-tree-parameters.range-actions-optimization.pst-model": "APPROXIMATED_INTEGERS",
        "extensions.open-rao-search-tree-parameters.costly-min-margin-parameters": {
            "shifted-violation-penalty": 10000.0,
            "shifted-violation-threshold": 5.0,
        },
    },
}

RAO_PROFILES = [
    {
        "name": "default",
        "overrides": {},
    },
    {
        "name": "extended search depth",
        "overrides": {
            "extensions.open-rao-search-tree-parameters.topological-actions-optimization.max-curative-search-tree-depth": False
        },
    },
]