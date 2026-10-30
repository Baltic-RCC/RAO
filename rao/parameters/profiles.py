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
        "name": "extended search depth",
        "overrides": {
            "extensions.open-rao-search-tree-parameters.topological-actions-optimization.max-curative-search-tree-depth": False
        },
    },
]