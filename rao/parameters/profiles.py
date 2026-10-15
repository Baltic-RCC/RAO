"""RAO parameter profiles tried in order by the optimizer worker.

The first profile is the default configuration. Each following one is a
fallback applied only when the previous attempt failed. Overrides use the
dotted paths understood by RaoSettingsManager.set().
"""

RAO_PROFILES = [
    {
        "name": "default",
        "overrides": {},
    },
    {
        "name": "relaxed-reactive-limits",
        "overrides": {
            "extensions.open-rao-search-tree-parameters.load-flow-and-sensitivity-computation"
            ".sensitivity-parameters.load-flow-parameters.useReactiveLimits": False,
        },
    },
]
