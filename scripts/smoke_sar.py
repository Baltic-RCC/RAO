"""Smoke test of rao.sar.generate_sar: reads RAO results from Elastic, generates the SAR with nc-profile-service,
prints the start of the profile and saves it to a file.

Hosts come from the settings (environment or config/.env): ELK_SERVER, ELK_TOKEN, ELK_SSL_VERIFY (and SSL_CERT_FILE
when verifying) and NC_PROFILE_SERVICE_URL.

    python scripts/smoke_sar.py --scenario-time 2026-09-23T22:30:00Z --time-horizon ID

Offline variants for when a service is not available: --sample-data uses built-in RAO results instead of Elastic,
--echo-service uses a local stub that returns the request instead of nc-profile-service (output is then the JSON
request sent to the service, not the SAR XML).
"""
import argparse
import json
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rao import sar  # noqa: E402

MAX_LINES = 80


def mrid(name: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_OID, name))


def doc(contingency, element, name, instant, flow, unit="ampere", batch="b2", timestamp="2026-11-05T11:00:00"):
    """RAO result document as stored by HandlerOptimizer: dotted column names, task headers under rmq."""
    return {
        "@timestamp": timestamp,
        "rmq": {"batch-id": batch, "contingency-id": contingency},
        "computationStatus": "default",
        "cnec.name": name,
        "cnec.networkElementId": f"_{element}",
        "cnec.contingencyId": contingency if instant == "curative" else None,
        "cnec.instant": instant,
        "cnec.thresholds.unit": unit,
        "cnec.thresholds.min": -1000,
        "cnec.thresholds.max": 1000,
        f"cnecResults.initial.{unit}.side1.flow": 5000,
        f"cnecResults.{instant}.{unit}.side1.flow": flow,
        f"cnecResults.{instant}.{unit}.side1.loading": abs(flow) / 1000,
    }


def sample_hits() -> list[dict]:
    line_a, line_b, trafo = mrid("line-a"), mrid("line-b"), mrid("trafo")
    co1, co2 = mrid("contingency-1"), mrid("contingency-2")
    return [
        # Contingency 1: line A at 90%, 3w transformer legs at 80% and 110% (reported once, worst leg)
        doc(co1, line_a, "Line A", "curative", 900), doc(co1, line_a, "Line A", "curative", 900),
        doc(co1, line_a, "Line A", "curative", -450, unit="megawatt"),
        doc(co1, f"{trafo}-Leg1", "Trafo", "curative", 800), doc(co1, f"{trafo}-Leg2", "Trafo", "curative", 1100),
        # Contingency 2: line B at 120% against the negative limit
        doc(co2, line_b, "Line B", "curative", -1200),
        # Base case of both runs, the worst one is reported
        doc(co1, line_a, "Line A", "preventive", 500), doc(co2, line_a, "Line A", "preventive", 600),
        # Older run of contingency 1 is ignored
        doc(co1, line_a, "Line A", "curative", 2000, batch="b1", timestamp="2026-11-05T10:00:00"),
        # Not an mRID, skipped with a warning
        doc(co1, "NOT-AN-MRID", "Broken", "curative", 100),
    ]


def use_sample_results() -> SimpleNamespace:
    def fake_scan(client, index, query):
        print(f"--- Elastic query (not executed), index {index}\n{json.dumps(query, indent=2)}\n")
        return [{"_source": hit} for hit in sample_hits()]

    sar.scan = fake_scan
    return SimpleNamespace(client=None)


class EchoService(BaseHTTPRequestHandler):
    """Stands in for nc-profile-service, returns the received request as the response."""

    def do_POST(self):
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if self.path != sar.SAR_ENDPOINT:
            self.send_error(404)
            return
        response = json.dumps(json.loads(body), indent=2).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *args):
        pass


def start_echo_service() -> str:
    server = HTTPServer(("127.0.0.1", 0), EchoService)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return f"http://127.0.0.1:{server.server_port}"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario-time", required=True, help="ISO timestamp, e.g. 2026-09-23T22:30:00Z")
    parser.add_argument("--time-horizon", required=True, help="e.g. ID")
    parser.add_argument("--service-url", help="nc-profile-service URL, default is the NC_PROFILE_SERVICE_URL setting")
    parser.add_argument("--output", help="file to save the response to, default SAR_<horizon>_<time>.xml")
    parser.add_argument("--sample-data", action="store_true", help="use built-in RAO results instead of Elastic")
    parser.add_argument("--echo-service", action="store_true", help="use a local stub instead of nc-profile-service")
    args = parser.parse_args()

    elastic = use_sample_results() if args.sample_data else None
    if args.echo_service:
        sar.conf.nc_profile_service_url = start_echo_service()
    elif args.service_url:
        sar.conf.nc_profile_service_url = args.service_url
    print(f"--- Results from: {'sample data' if args.sample_data else 'Elastic'}")
    print(f"--- nc-profile-service: {sar.conf.nc_profile_service_url}" + (" (echo stub)" if args.echo_service else ""))

    request = sar.SarRequest(scenario_time=args.scenario_time, time_horizon=args.time_horizon)
    content = sar.generate_sar(request, elastic=elastic)
    if content is None:
        raise SystemExit("No RAO results found, nothing to generate")

    extension = "json" if args.echo_service else "xml"
    output = Path(args.output or f"SAR_{request.time_horizon}_{request.scenario_time:%Y%m%dT%H%M}.{extension}")
    output.write_bytes(content)

    lines = content.decode().splitlines()
    print(f"--- Response ({len(content)} bytes, {len(lines)} lines)")
    print("\n".join(lines[:MAX_LINES]))
    if len(lines) > MAX_LINES:
        print(f"... {len(lines) - MAX_LINES} more lines")
    print(f"--- Saved to {output.resolve()}")


if __name__ == "__main__":
    main()
