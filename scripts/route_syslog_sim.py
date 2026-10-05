#!/usr/bin/env python3
"""Send simulated route-change syslog messages to a remote syslog receiver.

Generates the log lines the routelogbeat correlator correlates for one route
change, in the order and with the delays seen on a real system:

    t=0.000s   scheduler   main: Sending route: [[{'src': [...], 'dst': [...]}]]
    t=0.004s   magclientsrv INFO:interfaces.server:Received Dispatch Request. Client [ip:port], ... Method [route] ...
    t=2.441s   magnum      INFO:jsonrpc:Subscribe request. ...
    t=2.444s   magnum      INFO:subscription:Subscription Request Complete: ...
    t=12.535s  magrtrsrv   INFO:commands:Cmd. ... M [set.rx.route] ... multicast_ip ...
                           (one per --slab, 12ms apart)
    t=12.576s  slab #1     <slab> [dd.mm.yyyy hh:mm:ss.fff] W: exlwrp-lwrp: ... AuditSet:DST <n> ADDR:"<mcast>..."
    t=12.588s  slab #2     (one line per --slab)

The UUIDs, slab names, output (DST) numbers and multicast are all set on the
command line, or per route from a JSON scenario file (--scenario). Every
message is sent in real time at its offset, so the beat sees genuine delays.

The slab hostname reaches the beat through annotation.general.device_name,
which the upstream syslog pipeline fills in, so each slab line is sent with
the slab's name as the syslog HOSTNAME. Check that your pipeline maps it the
same way (it may annotate by source IP instead).

Examples:

    # One route to a remote receiver, defaults for everything else
    ./route_syslog_sim.py --target 10.9.0.69

    # Choose the route explicitly
    ./route_syslog_sim.py --target 10.9.0.69 \\
        --src 11111111-1111-1111-1111-111111111111 \\
        --dst 22222222-2222-2222-2222-222222222222 \\
        --slab sv7bc-slab027:4 --slab sv7bc-slab058:4 \\
        --multicast 239.32.111.55

    # Five routes, a new one every 3s (they overlap), 2x slower, +/-10% jitter
    ./route_syslog_sim.py --target 10.9.0.69 --count 5 --interval 3 --scale 2 --jitter 0.1

    # No scheduler log, magclientsrv opens the envelope instead (not partial)
    ./route_syslog_sim.py --target 10.9.0.69 --scheduler none --client-ip 10.103.40.46

    # Make the slab ADDR disagree with magrtrsrv (tests multicast_conflict)
    ./route_syslog_sim.py --target 10.9.0.69 --slab-multicast 239.1.1.1

    # Print what would be sent, with offsets, without sending
    ./route_syslog_sim.py --dry-run

Only the Python 3 standard library is used.
"""

from __future__ import annotations

import argparse
import json
import random
import socket
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

# Default offsets (seconds from the scheduler log), taken from the captured
# sample in beater/correlator/example_test.go.
DEFAULT_TIMING = {
    "scheduler": 0.0,
    "magclientsrv": 0.004,
    "subscribe": 2.441,
    "complete": 2.444,
    "magrtrsrv": 12.535,
    "slab": 12.576,   # first slab line
    "slab_gap": 0.012,  # added for each further slab line
}

FACILITIES = {
    "kern": 0, "user": 1, "daemon": 3, "local0": 16, "local1": 17, "local2": 18,
    "local3": 19, "local4": 20, "local5": 21, "local6": 22, "local7": 23,
}
SEVERITY_INFO = 6


@dataclass
class Slab:
    name: str
    output: int


@dataclass
class Route:
    src: str
    dst: str
    slabs: list[Slab]
    multicast: str
    slab_multicast: str | None = None  # None: same as multicast
    start: float = 0.0                   # offset of this route's first log
    client_ip: str = "10.103.40.46"      # magclientsrv "Client [ip:port]"
    client_port: int = 45662


@dataclass(order=True)
class Message:
    offset: float
    seq: int
    kind: str = field(compare=False)
    hostname: str = field(compare=False)
    tag: str = field(compare=False)
    body: str = field(compare=False)
    route_no: int = field(compare=False)


# --------------------------------------------------------------------------
# Log bodies. These must match the correlator parsers in beater/correlator.
# --------------------------------------------------------------------------

def scheduler_body(r: Route, variant: str) -> str:
    obj = f"{{'src': ['{r.src}'], 'dst': ['{r.dst}']}}"
    if variant == "v2":
        return f"dcpipes.jsonrpctcp: SENDING: {{'params': [[{obj}]], 'method': 'route'}}"
    return f"main: Sending route: [[{obj}]]"


def magclientsrv_body(r: Route) -> str:
    client = f"[{r.client_ip}]:{r.client_port}" if ":" in r.client_ip else f"{r.client_ip}:{r.client_port}"
    return (f"INFO:interfaces.server:Received Dispatch Request. Client [{client}], "
            f"Message ID [{random.randint(1, 99999)}], Method [route], "
            f"Parameters [[[{{'src': ['{r.src}'], 'dst': ['{r.dst}']}}]]].")


def subscribe_body(r: Route, shape: str) -> str:
    if shape == "subscription":
        return (f"INFO:jsonrpc:Subscribe request. Subscription [{{'dst': ['{r.dst}'], "
                f"'sub_dst': ['{r.src}']}}], User [admin], ID [{uuid.uuid4()}]")
    return (f"INFO:jsonrpc:Subscribe request. Dst [('{r.dst}',)], "
            f"Sub [('{r.src}',)], User [None], ID [None]")


def complete_body(r: Route) -> str:
    return (f"INFO:subscription:Subscription Request Complete: Routes [1-1]: "
            f"[{{'dst': ['{r.dst}'], 'sub_dst': ['{r.src}']}}]")


def magrtrsrv_body(r: Route, slab: Slab, udp_port: int, source_ip: str) -> str:
    return (f"INFO:commands:Cmd. D [{random.randint(1000, 9999)}], N [{slab.name}], "
            f"M [set.rx.route], A [[[{{'dest': {{'output': {slab.output}, 'port_type': 1, "
            f"'stream_type': 2}}, 'sources': [{{'sfp': 1, 'multicast_ip': '{r.multicast}', "
            f"'udp_port': {udp_port}, 'source_ips': ['{source_ip}']}}]}}]]], K [{{}}]")


def slab_body(r: Route, slab: Slab, at: datetime) -> str:
    addr = r.slab_multicast or r.multicast
    stamp = at.strftime("%d.%m.%Y %H:%M:%S.") + f"{at.microsecond // 1000:03d}"
    return (f"{slab.name} [{stamp}] W: exlwrp-lwrp: info: username:legacy_login "
            f"AuditSet:DST {slab.output} ADDR:\"{addr};sync-time=3000\"")


# --------------------------------------------------------------------------
# Syslog framing
# --------------------------------------------------------------------------

def frame(msg: Message, body: str, at: datetime, fmt: str, pri: int) -> str:
    # ISO 8601, UTC, millisecond precision, e.g. 2026-10-02T17:47:11.123Z
    ts = at.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    if fmt == "rfc5424":
        app = msg.tag or "-"
        return f"<{pri}>1 {ts} {msg.hostname} {app} - - - {body}"
    # RFC 3164 layout, with an ISO 8601 timestamp in place of "Mmm dd hh:mm:ss".
    tag = f"{msg.tag}: " if msg.tag else ""
    return f"<{pri}>{ts} {msg.hostname} {tag}{body}"


class Sender:
    def __init__(self, host: str, port: int, proto: str, octet_counting: bool):
        self.addr = (host, port)
        self.proto = proto
        self.octet_counting = octet_counting
        if proto == "udp":
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            self.sock = socket.create_connection(self.addr, timeout=10)

    def send(self, line: str) -> None:
        data = line.encode("utf-8")
        if self.proto == "udp":
            self.sock.sendto(data, self.addr)
        elif self.octet_counting:
            self.sock.sendall(f"{len(data)} ".encode() + data)
        else:
            self.sock.sendall(data + b"\n")

    def close(self) -> None:
        self.sock.close()


# --------------------------------------------------------------------------
# Building the timeline
# --------------------------------------------------------------------------

def parse_slab(spec: str) -> Slab:
    name, sep, out = spec.rpartition(":")
    if not sep or not name or not out.isdigit():
        raise argparse.ArgumentTypeError(f"slab must be NAME:OUTPUT, got {spec!r}")
    return Slab(name, int(out))


def check_uuid(s: str) -> str:
    try:
        return str(uuid.UUID(s))
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a UUID: {s!r}")


def build_messages(routes: list[Route], a: argparse.Namespace) -> list[Message]:
    timing = dict(DEFAULT_TIMING)
    for name in timing:
        v = getattr(a, f"at_{name}")
        if v is not None:
            timing[name] = v

    msgs: list[Message] = []
    seq = 0
    # Per route: the previous step's nominal and actual offsets. Scale and
    # jitter apply to the gap between consecutive steps, so a route's own logs
    # never reorder (subscribe stays before complete, magrtrsrv before slab).
    prev_nominal = prev_actual = 0.0

    def add(route_no: int, base: float, offset: float, kind: str, host: str, tag: str, body: str):
        nonlocal seq, prev_nominal, prev_actual
        gap = max(offset - prev_nominal, 0.0) * a.scale
        if a.jitter:
            gap *= 1 + random.uniform(-a.jitter, a.jitter)
        prev_nominal, prev_actual = offset, prev_actual + gap
        msgs.append(Message(base * a.scale + prev_actual, seq, kind, host, tag, body, route_no))
        seq += 1

    for i, r in enumerate(routes, 1):
        base = r.start
        prev_nominal = prev_actual = 0.0
        if a.scheduler != "none":
            add(i, base, timing["scheduler"], "scheduler", a.scheduler_host, a.scheduler_tag,
                scheduler_body(r, a.scheduler))
        if not a.no_magclientsrv:
            add(i, base, timing["magclientsrv"], "magclientsrv", a.magclientsrv_host, a.magclientsrv_tag,
                magclientsrv_body(r))
        if a.subscribe != "none":
            add(i, base, timing["subscribe"], "magnum", a.magnum_host, a.magnum_tag,
                subscribe_body(r, a.subscribe))
        if not a.no_complete:
            add(i, base, timing["complete"], "magnum", a.magnum_host, a.magnum_tag, complete_body(r))
        if a.magrtrsrv != "none":
            # One set.rx.route per slab (unless "first"), spaced like the slab
            # lines so each slab's magrtrsrv log precedes it.
            targets = r.slabs if a.magrtrsrv == "all" else r.slabs[:1]
            for n, slab in enumerate(targets):
                add(i, base, timing["magrtrsrv"] + n * timing["slab_gap"], "magrtrsrv", a.magrtrsrv_host, a.magrtrsrv_tag,
                    magrtrsrv_body(r, slab, a.udp_port, a.source_ip))
        for n, slab in enumerate(r.slabs):
            # body is rendered at send time so its in-line timestamp is real
            add(i, base, timing["slab"] + n * timing["slab_gap"], "slab", slab.name, a.slab_tag,
                f"__SLAB__{n}")

    msgs.sort()
    return msgs


def routes_from_args(a: argparse.Namespace) -> list[Route]:
    slabs = a.slab or [Slab("sv7bc-slab027", 4), Slab("sv7bc-slab058", 4)]
    routes = []
    for n in range(a.count):
        # Explicit UUIDs are reused for every route; otherwise each is new.
        routes.append(Route(
            src=a.src or str(uuid.uuid4()),
            dst=a.dst or str(uuid.uuid4()),
            slabs=slabs,
            multicast=a.multicast,
            slab_multicast=a.slab_multicast,
            start=n * a.interval,
            client_ip=a.client_ip,
            client_port=a.client_port,
        ))
    return routes


def routes_from_scenario(path: str, a: argparse.Namespace) -> list[Route]:
    """Load routes from JSON. Missing fields fall back to the CLI values.

    {
      "routes": [
        {"src": "...", "dst": "...", "multicast": "239.32.111.55",
         "slabs": [{"name": "sv7bc-slab027", "output": 4}, "sv7bc-slab058:4"],
         "slab_multicast": "239.1.1.1", "start": 0,
         "client_ip": "10.103.40.46", "client_port": 45662}
      ]
    }
    """
    with open(path) as f:
        doc = json.load(f)
    entries = doc["routes"] if isinstance(doc, dict) else doc
    default_slabs = a.slab or [Slab("sv7bc-slab027", 4), Slab("sv7bc-slab058", 4)]
    routes = []
    for n, e in enumerate(entries):
        slabs = [parse_slab(s) if isinstance(s, str) else Slab(s["name"], int(s["output"]))
                 for s in e.get("slabs", [])] or default_slabs
        routes.append(Route(
            src=check_uuid(e["src"]) if "src" in e else (a.src or str(uuid.uuid4())),
            dst=check_uuid(e["dst"]) if "dst" in e else (a.dst or str(uuid.uuid4())),
            slabs=slabs,
            multicast=e.get("multicast", a.multicast),
            slab_multicast=e.get("slab_multicast", a.slab_multicast),
            start=float(e.get("start", n * a.interval)),
            client_ip=e.get("client_ip", a.client_ip),
            client_port=int(e.get("client_port", a.client_port)),
        ))
    return routes


# --------------------------------------------------------------------------

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Send simulated route-change syslog messages for routelogbeat end-to-end tests.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Examples:", 1)[1].split("Only the Python", 1)[0],
    )
    g = p.add_argument_group("receiver")
    g.add_argument("--target", default="127.0.0.1", help="syslog receiver host (default 127.0.0.1)")
    g.add_argument("--port", type=int, default=514, help="syslog receiver port (default 514)")
    g.add_argument("--proto", choices=["udp", "tcp"], default="udp")
    g.add_argument("--octet-counting", action="store_true",
                   help="TCP: RFC 6587 octet-counting framing instead of newline")
    g.add_argument("--format", choices=["rfc3164", "rfc5424"], default="rfc3164",
                   help="syslog header layout; both use an ISO 8601 UTC timestamp with milliseconds")
    g.add_argument("--facility", choices=sorted(FACILITIES), default="local0")
    g.add_argument("--dry-run", action="store_true", help="print the messages, don't send")

    g = p.add_argument_group("route")
    g.add_argument("--src", type=check_uuid, help="source UUID (default: random per route)")
    g.add_argument("--dst", type=check_uuid, help="destination UUID (default: random per route)")
    g.add_argument("--slab", type=parse_slab, action="append", metavar="NAME:OUTPUT",
                   help="slab name and output/DST number; repeat per slab "
                        "(default sv7bc-slab027:4 and sv7bc-slab058:4)")
    g.add_argument("--multicast", default="239.32.111.55", help="route multicast (default 239.32.111.55)")
    g.add_argument("--slab-multicast",
                   help="ADDR to put in the slab lines instead of --multicast (tests multicast_conflict)")
    g.add_argument("--udp-port", type=int, default=5004, help="magrtrsrv udp_port (default 5004)")
    g.add_argument("--source-ip", default="10.12.42.89", help="magrtrsrv source_ips entry")
    g.add_argument("--client-ip", default="10.103.40.46", help="magclientsrv client IP (default 10.103.40.46)")
    g.add_argument("--client-port", type=int, default=45662, help="magclientsrv client port (default 45662)")
    g.add_argument("--scenario", metavar="FILE", help="JSON file with a list of routes (see source)")

    g = p.add_argument_group("which logs")
    g.add_argument("--scheduler", choices=["v1", "v2", "none"], default="v1",
                   help="scheduler line format, or none (magclientsrv, or magnum if that is off too, opens the envelope)")
    g.add_argument("--no-magclientsrv", action="store_true",
                   help="omit the magclientsrv Received Dispatch Request line (it is optional on real systems)")
    g.add_argument("--subscribe", choices=["dst_sub", "subscription", "none"], default="dst_sub",
                   help="magnum Subscribe request shape")
    g.add_argument("--no-complete", action="store_true", help="omit the Subscription Request Complete line")
    g.add_argument("--magrtrsrv", choices=["all", "first", "none"], default="all",
                   help="send set.rx.route for every slab (default), only the first slab, or none")

    # The tag is the syslog process name: TAG in "HOST TAG: MSG" (rfc3164),
    # APP-NAME (rfc5424). Pass "" to omit it.
    g = p.add_argument_group("syslog hostnames and process names")
    g.add_argument("--scheduler-host", default="scheduler")
    g.add_argument("--scheduler-tag", default="route-scheduler", help="process name (default route-scheduler)")
    g.add_argument("--magclientsrv-host", default="magnum")
    g.add_argument("--magclientsrv-tag", default="magclientsrv", help="process name (default magclientsrv)")
    g.add_argument("--magnum-host", default="magnum")
    g.add_argument("--magnum-tag", default="magnum-api", help="process name (default magnum-api)")
    g.add_argument("--magrtrsrv-host", default="magnum")
    g.add_argument("--magrtrsrv-tag", default="magrtrsrv", help="process name (default magrtrsrv)")
    g.add_argument("--slab-tag", default="exlwrp",
                   help="process name (default exlwrp); slab lines use the slab name as hostname")

    g = p.add_argument_group("timing")
    g.add_argument("--count", type=int, default=1, help="number of routes (default 1)")
    g.add_argument("--interval", type=float, default=20.0,
                   help="seconds between route starts (default 20; less than ~12.6 overlaps routes)")
    g.add_argument("--scale", type=float, default=1.0, help="multiply every delay and route start (e.g. 0.1 for a quick run)")
    g.add_argument("--jitter", type=float, default=0.0, help="random +/- fraction applied to each gap between a route's logs")
    g.add_argument("--seed", type=int, help="random seed, for repeatable jitter and UUIDs")
    for name, v in DEFAULT_TIMING.items():
        g.add_argument(f"--at-{name.replace('_', '-')}", dest=f"at_{name}", type=float, metavar="SEC",
                       help=f"offset for {name} (default {v})")

    a = p.parse_args(argv)
    if a.count < 1:
        p.error("--count must be at least 1")
    return a


def main(argv: list[str]) -> int:
    a = parse_args(argv)
    if a.seed is not None:
        random.seed(a.seed)
        uuid.uuid4 = lambda: uuid.UUID(int=random.getrandbits(128), version=4)  # type: ignore[assignment]

    routes = routes_from_scenario(a.scenario, a) if a.scenario else routes_from_args(a)
    msgs = build_messages(routes, a)
    pri = FACILITIES[a.facility] * 8 + SEVERITY_INFO

    for i, r in enumerate(routes, 1):
        slabs = ", ".join(f"{s.name}:{s.output}" for s in r.slabs)
        extra = f" slab_multicast={r.slab_multicast}" if r.slab_multicast else ""
        print(f"route {i}: src={r.src} dst={r.dst} multicast={r.multicast}{extra} slabs=[{slabs}] "
              f"client={r.client_ip}:{r.client_port}")
    print(f"{len(msgs)} messages over {msgs[-1].offset:.3f}s -> "
          f"{'(dry run)' if a.dry_run else f'{a.proto}://{a.target}:{a.port}'} ({a.format})\n")

    try:
        sender = None if a.dry_run else Sender(a.target, a.port, a.proto, a.octet_counting)
    except OSError as e:
        print(f"cannot connect to {a.target}:{a.port}: {e}", file=sys.stderr)
        return 1
    t0 = time.monotonic()
    wall0 = datetime.now(timezone.utc)
    try:
        for m in msgs:
            if sender:
                wait = m.offset - (time.monotonic() - t0)
                if wait > 0:
                    time.sleep(wait)
                at = datetime.now(timezone.utc)
            else:
                at = wall0 + timedelta(seconds=m.offset)
            r = routes[m.route_no - 1]
            body = (slab_body(r, r.slabs[int(m.body[len("__SLAB__"):])], at)
                    if m.body.startswith("__SLAB__") else m.body)
            line = frame(m, body, at, a.format, pri)
            if sender:
                sender.send(line)
            print(f"+{m.offset:7.3f}s  route {m.route_no}  {m.kind:<12}  {line}")
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except OSError as e:
        print(f"send failed: {e}", file=sys.stderr)
        return 1
    finally:
        if sender:
            sender.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
