"""
build-rd-crac: build a JSON CRAC with redispatching injection range actions.

    build-rd-crac --rows export.csv --costs costs.yaml [--network model.xiidm]
                  [--base-crac crac.json] --out crac_out.json
"""
import argparse
import json
import sys
from pathlib import Path
import pypowsybl
from loguru import logger
from rao.crac.redispatch.builder import (
    CRAC_VERSION, DEFAULT_INSTANT, build_crac, build_injection_range_actions, crac_to_json, merge_into_crac,
    validate_crac,
)
from rao.crac.redispatch.costs import CostConfig
from rao.crac.redispatch.sources import CsvRowSource
from rao.parameters.loadflow import CGMES_IMPORT_PARAMETERS


def load_network(path: str | Path) -> pypowsybl.network.Network:
    """Load a network model, CGMES archives use the repository CGMES import parameters."""
    path = Path(path)
    parameters = CGMES_IMPORT_PARAMETERS if path.suffix.lower() == ".zip" else None
    logger.info(f"Loading network model: {path}")
    return pypowsybl.network.load(str(path), parameters=parameters)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="build-rd-crac",
        description="Convert redispatching remedial-action rows into OpenRAO injectionRangeActions (JSON CRAC).")
    parser.add_argument("--rows", required=True, help="RCC remedial-action export (CSV)")
    parser.add_argument("--costs", help="cost config (YAML or JSON) keyed by unit action id; built-in defaults if omitted")
    parser.add_argument("--network", help="network model; enables element id resolution, Pmin/Pmax fallback "
                                          "and validation by OpenRAO import")
    parser.add_argument("--base-crac", help="existing JSON CRAC to merge the actions into")
    parser.add_argument("--out", required=True, help="output JSON CRAC")
    parser.add_argument("--instant", default=DEFAULT_INSTANT, help="usage rule instant (default: %(default)s)")
    parser.add_argument("--contingency", action="append", dest="contingencies", metavar="ID",
                        help="emit onContingencyStateUsageRules for this contingency (repeatable, needs --base-crac)")
    parser.add_argument("--crac-id", default="RD_CRAC", help="CRAC id when no base CRAC is given (default: %(default)s)")
    parser.add_argument("--crac-version", default=CRAC_VERSION,
                        help="CRAC JSON version when no base CRAC is given (default: %(default)s)")
    parser.add_argument("--log-level", default="INFO", help="log level (default: %(default)s)")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logger.remove()
    logger.add(sys.stderr, level=args.log_level.upper())

    if args.contingencies and not args.base_crac:
        logger.error("--contingency needs --base-crac, a standalone CRAC holds no contingencies")
        return 2

    rows = CsvRowSource(args.rows).read()
    costs = CostConfig.from_file(args.costs) if args.costs else CostConfig()
    network = load_network(args.network) if args.network else None

    result = build_injection_range_actions(rows, costs=costs, network=network, instant=args.instant,
                                           contingency_ids=args.contingencies)
    if args.base_crac:
        base = json.loads(Path(args.base_crac).read_text(encoding="utf-8"))
        crac = merge_into_crac(base, result.actions)
    else:
        crac = build_crac(result.actions, crac_id=args.crac_id, version=args.crac_version)

    if network is not None:
        validate_crac(network, crac)
    else:
        logger.warning("No network given, the CRAC was not validated by OpenRAO import")

    Path(args.out).write_text(crac_to_json(crac), encoding="utf-8")
    print(result.summary())
    print(f"CRAC written to: {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
