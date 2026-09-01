#!/usr/bin/env python3
"""env-tool — cluster-aware env management for 2-node DGX Spark recipes.

Discovers the inter-node RoCE fabric of a head/worker DGX Spark pair over
SSH, and regenerates a recipe repository's local env file from its upstream
example plus machine-derived facts plus a small user-preferences file.

Subcommands:

  merge EXAMPLE LOCAL -o OUTPUT
      Legacy overlay: replace active KEY=VALUE lines of EXAMPLE with values
      from LOCAL, write to OUTPUT. Comments preserved.

  probe [--head TARGET] [--worker TARGET] [--verify] [--verify-rdma]
      SSH to both nodes, discover inter-node RoCE links (UP netdevs with an
      ibdev mapping, shared IPv4 subnet, valid RoCE v2 GID on both ends),
      resolve per-node GID indices, cache facts to
      $ENV_TOOL_CONFIG_DIR/facts.json (default ~/.config/env-tool).

  facts
      Print the cached cluster facts.

  apply [REPO] [--check] [--prefs FILE] [--adapter NAME] [--probe]
      Regenerate a recipe repo's local env file from its upstream example
      (.env.example / .env.sample / .env.*.example). Without REPO, operates
      on the git checkout containing the current directory (cd into the
      repo after git pull, then run `env-tool apply --check`). Implicit
      invocations are guarded: the target must be a git repo with a known
      recipe signature, and home/root directories are always refused; pass
      an explicit path to override the guards. The recipe format is
      auto-detected; fact keys are filled from cached facts, user preference
      keys from .env.local (gitignore-safe: auto-added to .git/info/exclude).
      Uses a three-way merge when the pre-pull example is available from git
      (ORIG_HEAD): user-customized values survive, untouched values adopt
      upstream's new defaults, keys activated by the user but commented
      upstream stay active. Prints a drift report; --check exits 1 when the
      file would change. Comment blocks and line order are preserved
      byte-for-byte; only values change, in place.
"""

import argparse
import ipaddress
import json
import os
import re
import shutil
import socket
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CONFIG_DIR_ENV = "ENV_TOOL_CONFIG_DIR"
EXAMPLE_NAMES = (".env.example", ".env.sample")
PREFS_NAME = ".env.local"

ACTIVE_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)=(.*)$")
COMMENTED_RE = re.compile(r"^#\s?([A-Za-z_][A-Za-z0-9_]*)=(.*)$")


def config_dir() -> Path:
    base = os.environ.get(CONFIG_DIR_ENV)
    root = Path(base) if base else Path.home() / ".config" / "env-tool"
    root.mkdir(parents=True, exist_ok=True)
    return root


def load_json(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return None


def save_json(path: Path, data: Dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# env file parsing
# ---------------------------------------------------------------------------


def split_value(raw: str) -> Tuple[str, str, str]:
    """Split the text after '=' into (value, quote_char, trailing_comment)."""
    quote = ""
    if raw[:1] in ("\"", "'"):
        close = raw.find(raw[0], 1)
        if close != -1:
            value, rest = raw[1:close], raw[close + 1 :]
            m = re.match(r"^(\s+#.*)$", rest)
            return value, raw[0], m.group(1) if m else ""
    m = re.match(r"^(.*?)(\s+#.*)$", raw)
    if m:
        return m.group(1), "", m.group(2)
    return raw, "", ""


class EnvLine:
    __slots__ = ("raw", "key", "active", "value", "quote", "comment")

    def __init__(self, raw: str) -> None:
        self.raw = raw
        self.key = ""
        self.active = False
        self.value = ""
        self.quote = ""
        self.comment = ""
        m = ACTIVE_RE.match(raw)
        if m:
            self.key, self.active = m.group(1), True
            self.value, self.quote, self.comment = split_value(m.group(2))
            return
        m = COMMENTED_RE.match(raw)
        if m:
            self.key, self.active = m.group(1), False
            self.value, self.quote, self.comment = split_value(m.group(2))

    def render(self, value: str, active: bool) -> str:
        out = "" if active else "# "
        if self.quote:
            out += f"{self.key}={self.quote}{value}{self.quote}"
        else:
            out += f"{self.key}={value}"
        return out + self.comment

    def render_new(self, key: str, value: str, quoted: bool) -> str:
        if quoted:
            return f"{key}=\"{value}\""
        return f"{key}={value}"


def parse_env(text: str) -> List[EnvLine]:
    return [EnvLine(raw) for raw in text.splitlines()]


def active_map(lines: List[EnvLine]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for line in lines:
        if line.active and line.key:
            out.setdefault(line.key, line.value)
    return out


def present_keys(lines: List[EnvLine]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    for line in lines:
        if line.key:
            out.setdefault(line.key, line.active)
    return out


def parse_kv(text: str) -> Dict[str, str]:
    return active_map(parse_env(text))


# ---------------------------------------------------------------------------
# probe: node discovery over ssh
# ---------------------------------------------------------------------------

NODE_SCRIPT = r"""
echo "===IFACES4==="
ip -o -4 addr show 2>/dev/null
echo "===LINKS==="
ip -o link show 2>/dev/null
echo "===IBDEV2NETDEV==="
ibdev2netdev 2>/dev/null || true
echo "===GIDS==="
for d in /sys/class/infiniband/*; do
  [ -d "$d" ] || continue
  dev=${d##*/}
  for p in "$d"/ports/*; do
    [ -d "$p" ] || continue
    port=${p##*/}
    for g in "$p"/gids/*; do
      [ -f "$g" ] || continue
      i=${g##*/}
      t=$(cat "$p/gid_attrs/types/$i" 2>/dev/null)
      v=$(cat "$g" 2>/dev/null)
      echo "GID $dev $port $i $t $v"
    done
  done
done
echo "===DONE==="
"""

IFACE4_RE = re.compile(r"^\d+:\s+(\S+)\s+inet\s+(\d+(?:\.\d+){3})/(\d+)\s")
LINK_RE = re.compile(
    r"^\d+:\s+([^:@\s]+)(?:@\S+)?:\s+<([^>]*)>.*?\bmtu\s+(\d+)\b.*?\bstate\s+(\S+)"
)
IBDEV_RE = re.compile(r"^(\S+)\s+port\s+(\d+)\s*==>\s*(\S+)\s*\((Up|Down)\)")


def run_target(target: str, script: str, timeout: int = 60) -> str:
    if target == "localhost":
        cmd = ["sh", "-s"]
    else:
        cmd = [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=15",
            target,
            "sh",
            "-s",
        ]
    proc = subprocess.run(
        cmd, input=script, text=True, capture_output=True, timeout=timeout
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"target {target!r} failed (rc={proc.returncode}): "
            f"{proc.stderr.strip()[:400]}"
        )
    return proc.stdout


def parse_node_output(out: str) -> Dict[str, Any]:
    section = ""
    node: Dict[str, Any] = {
        "ifaces": {},
        "links": {},
        "ibmap": {},
        "gids": {},
    }
    for raw in out.splitlines():
        line = raw.strip()
        for name in ("IFACES4", "LINKS", "IBDEV2NETDEV", "GIDS", "DONE"):
            if line == f"==={name}===":
                section = name
                break
        else:
            if section == "IFACES4":
                m = IFACE4_RE.match(raw)
                if m:
                    node["ifaces"].setdefault(m.group(1), []).append(
                        (m.group(2), int(m.group(3)))
                    )
            elif section == "LINKS":
                m = LINK_RE.match(raw)
                if m:
                    flags = m.group(2)
                    carrier = "UP" in flags and "NO-CARRIER" not in flags
                    node["links"][m.group(1)] = {
                        "carrier": carrier,
                        "mtu": int(m.group(3)),
                        "state": m.group(4),
                    }
            elif section == "IBDEV2NETDEV":
                m = IBDEV_RE.match(raw)
                if m:
                    node["ibmap"][(m.group(1), int(m.group(2)))] = {
                        "netdev": m.group(3),
                        "up": m.group(4) == "Up",
                    }
            elif section == "GIDS":
                parts = raw.split()
                if len(parts) >= 6 and parts[0] == "GID":
                    try:
                        idx = int(parts[3])
                    except ValueError:
                        continue
                    node["gids"].setdefault((parts[1], int(parts[2])), []).append(
                        (idx, " ".join(parts[4:-1]), parts[-1])
                    )
    return node


def netdev_to_ibdev(node: Dict[str, Any]) -> Dict[str, Tuple[str, int]]:
    out: Dict[str, Tuple[str, int]] = {}
    for (dev, port), info in node["ibmap"].items():
        if info["up"]:
            out.setdefault(info["netdev"], (dev, port))
    return out


def v4_mapped_gid(ip: str) -> str:
    hexip = socket.inet_aton(ip).hex()
    return "0000:0000:0000:0000:0000:ffff:%s:%s" % (hexip[:4], hexip[4:])


def find_gid_index(node: Dict[str, Any], dev: str, port: int, ip: str) -> Optional[int]:
    want = v4_mapped_gid(ip)
    for idx, gtype, gid in node["gids"].get((dev, port), []):
        if gtype == "RoCE v2" and gid.lower() == want:
            return idx
    return None


def deduce_links(
    head: Dict[str, Any], worker: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], List[str]]:
    links: List[Dict[str, Any]] = []
    notes: List[str] = []
    head_ib = netdev_to_ibdev(head)
    worker_ib = netdev_to_ibdev(worker)
    for hif, haddrs in head["ifaces"].items():
        if hif not in head_ib:
            continue
        if not head["links"].get(hif, {}).get("carrier"):
            continue
        hdev, hport = head_ib[hif]
        for wif, waddrs in worker["ifaces"].items():
            if wif not in worker_ib:
                continue
            if not worker["links"].get(wif, {}).get("carrier"):
                continue
            wdev, wport = worker_ib[wif]
            for hip, hplen in haddrs:
                hnet = ipaddress.ip_network(f"{hip}/{hplen}", strict=False)
                for wip, wplen in waddrs:
                    wnet = ipaddress.ip_network(f"{wip}/{wplen}", strict=False)
                    if hnet != wnet:
                        continue
                    hgid = find_gid_index(head, hdev, hport, hip)
                    wgid = find_gid_index(worker, wdev, wport, wip)
                    if hgid is None or wgid is None:
                        notes.append(
                            f"skipped {hif}<->{wif} on {hnet}: no RoCE v2 GID "
                            f"matching {hip}/{wip} (head gid={hgid} worker gid={wgid})"
                        )
                        continue
                    links.append(
                        {
                            "subnet": str(hnet),
                            "head_if": hif,
                            "head_ip": hip,
                            "head_hca": hdev,
                            "head_gid": hgid,
                            "worker_if": wif,
                            "worker_ip": wip,
                            "worker_hca": wdev,
                            "worker_gid": wgid,
                            "mtu": min(
                                head["links"][hif]["mtu"],
                                worker["links"][wif]["mtu"],
                            ),
                        }
                    )
    links.sort(key=lambda l: ipaddress.ip_address(l["head_ip"]))
    return links, notes


def probe_nodes(head_target: str, worker_target: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    head = parse_node_output(run_target(head_target, NODE_SCRIPT))
    worker = parse_node_output(run_target(worker_target, NODE_SCRIPT))
    return head, worker


def build_facts(
    head_target: str,
    worker_target: str,
    primary_subnet: Optional[str] = None,
) -> Dict[str, Any]:
    head, worker = probe_nodes(head_target, worker_target)
    links, notes = deduce_links(head, worker)
    if not links:
        raise RuntimeError(
            "no inter-node RoCE links found; notes:\n  " + "\n  ".join(notes or ["(none)"])
        )
    primary = 0
    if primary_subnet:
        for i, link in enumerate(links):
            if link["subnet"] == primary_subnet:
                primary = i
                break
        else:
            notes.append(f"primary_subnet {primary_subnet} matched no link; using first")
    return {
        "probed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "head": head_target,
        "worker": worker_target,
        "primary": primary,
        "links": links,
        "notes": notes,
    }


def verify_link(head_target: str, worker_target: str, link: Dict[str, Any], rdma: bool) -> List[str]:
    issues: List[str] = []
    hip, wip, hif = link["head_ip"], link["worker_ip"], link["head_if"]
    hca, gid = link["head_hca"], link["head_gid"]
    script = (
        f"ping -c1 -W2 -I {hif} {wip} >/dev/null 2>&1 || echo FAIL_SMALL\n"
        f"ping -c1 -W2 -M do -s 8972 -I {hif} {wip} >/dev/null 2>&1 || echo FAIL_JUMBO\n"
    )
    out = run_target(head_target, script, timeout=30)
    for marker in ("FAIL_SMALL", "FAIL_JUMBO"):
        if marker in out:
            issues.append(f"{link['subnet']}: {marker.lower().lstrip('fail_')} ping failed")
    if link["mtu"] < 9000:
        issues.append(f"{link['subnet']}: MTU {link['mtu']} < 9000 (jumbo not configured)")
    if rdma:
        try:
            run_target(
                worker_target,
                f"command -v ib_write_bw >/dev/null || echo NO_PERFTEST\n",
                timeout=15,
            )
            run_target(
                head_target,
                f"command -v ib_write_bw >/dev/null || echo NO_PERFTEST\n",
                timeout=15,
            )
            run_target(
                worker_target,
                f"nohup ib_write_bw -d {hca} -x {gid} -R >/dev/null 2>&1 &\nsleep 1\n",
                timeout=15,
            )
            bw = run_target(
                head_target,
                f"ib_write_bw -d {hca} -x {gid} -R 2>&1 | tail -1\n",
                timeout=90,
            )
            m = re.search(r"([\d.]+)\s*$", bw.strip().splitlines()[-1]) if bw.strip() else None
            if not m:
                issues.append(f"{link['subnet']}: RDMA ib_write_bw failed")
        except RuntimeError as exc:
            issues.append(f"{link['subnet']}: RDMA check error: {exc}")
    return issues


def print_facts(facts: Dict[str, Any]) -> None:
    p = facts["links"][facts["primary"]]
    print(
        f"cluster: {facts['head']} (head) <-> {facts['worker']} (worker)\n"
        f"probed:  {facts['probed_at']}"
    )
    for i, link in enumerate(facts["links"]):
        tag = " [primary]" if i == facts["primary"] else ""
        print(
            f"link {i}{tag}: {link['subnet']} mtu {link['mtu']}\n"
            f"  head:   {link['head_if']} {link['head_ip']} hca {link['head_hca']} gid {link['head_gid']}\n"
            f"  worker: {link['worker_if']} {link['worker_ip']} hca {link['worker_hca']} gid {link['worker_gid']}"
        )
    if len(facts["links"]) > 1:
        print(
            f"note: {len(facts['links'])} inter-node links; dual-HCA merge "
            f"(NCCL_IB_HCA=comma list) is possible"
        )
    for note in facts.get("notes", []):
        print(f"note: {note}")
    print(
        "canonical: head_ip=%s worker_ip=%s head_if=%s worker_if=%s "
        "head_hca=%s worker_hca=%s gid=%s"
        % (
            p["head_ip"],
            p["worker_ip"],
            p["head_if"],
            p["worker_if"],
            p["head_hca"],
            p["worker_hca"],
            p["head_gid"],
        )
    )


def load_config() -> Dict[str, Any]:
    return load_json(config_dir() / "config.json") or {}


# ---------------------------------------------------------------------------
# adapters: canonical facts -> recipe-specific keys
# ---------------------------------------------------------------------------


def detect_adapter(lines: List[EnvLine]) -> str:
    present = present_keys(lines)
    if "HEAD_CX7_IF" in present:
        return "glm"
    if "IFACE" in present and "IB_HCA" in present:
        return "qwen"
    if "WORKER_HOST" in present and "MASTER_ADDR" in present:
        active = active_map(lines)
        hca = active.get("NCCL_IB_HCA", "")
        if "," in hca or "NCCL_IB_MERGE_NICS" in active:
            return "dspark-dual"
        return "dspark"
    return "plain"


def adapter_values(
    name: str, facts: Dict[str, Any], present: Dict[str, bool]
) -> Tuple[Dict[str, Tuple[str, bool]], List[str]]:
    links: List[Dict[str, Any]] = facts["links"]
    link = links[facts["primary"]]
    out: Dict[str, Tuple[str, bool]] = {}
    notes: List[str] = []

    def put(key: str, value: str, activate: bool = False, required: bool = True) -> None:
        if key in present:
            out[key] = (value, activate)
        elif required:
            notes.append(f"{name}: key {key} not present in example; value dropped: {value}")

    h, w = link["head_ip"], link["worker_ip"]

    if name in ("dspark", "dspark-dual"):
        put("WORKER_HOST", w)
        put("MASTER_ADDR", h)
        put("VLLM_HOST_IP", h)
        put("WORKER_VLLM_HOST_IP", w)
        if name == "dspark-dual":
            put("NCCL_IB_HCA", ",".join(l["head_hca"] for l in links))
            put("NCCL_SOCKET_IFNAME", ",".join(l["head_if"] for l in links))
            gids = {l["head_gid"] for l in links} | {l["worker_gid"] for l in links}
            if len(gids) == 1:
                put("NCCL_IB_GID_INDEX", str(gids.pop()))
            else:
                notes.append(
                    "dspark-dual: links/nodes do not share one RoCE v2 GID index; "
                    "not pinning NCCL_IB_GID_INDEX"
                )
            notes.append(
                "dspark-dual: GID pinned from probe; re-probe after NIC/host reboots"
            )
        else:
            put("NCCL_IB_HCA", link["head_hca"])
            put("NCCL_SOCKET_IFNAME", link["head_if"])
            put("TP_SOCKET_IFNAME", link["head_if"])
            put("GLOO_SOCKET_IFNAME", link["head_if"])
            notes.append("dspark: GID left to the recipe's boot-time auto-resolver")
        if link["worker_hca"] != link["head_hca"]:
            put("WORKER_NCCL_IB_HCA", link["worker_hca"], activate=True, required=False)
        if link["worker_if"] != link["head_if"]:
            for key in (
                "WORKER_NCCL_SOCKET_IFNAME",
                "WORKER_TP_SOCKET_IFNAME",
                "WORKER_GLOO_SOCKET_IFNAME",
            ):
                put(key, link["worker_if"], activate=True, required=False)
    elif name == "glm":
        put("HEAD_IP", h)
        put("WORKER_IP", w)
        put("HEAD_CX7_IF", link["head_if"])
        put("WORKER_CX7_IF", link["worker_if"])
        put("HEAD_CX7_IB", link["head_hca"])
        put("WORKER_CX7_IB", link["worker_hca"])
        if link["head_gid"] == link["worker_gid"]:
            put("NCCL_IB_GID_INDEX", str(link["head_gid"]))
        else:
            put("HEAD_GID", str(link["head_gid"]), activate=True)
            put("WORKER_GID", str(link["worker_gid"]), activate=True)
        notes.append("glm: GID indices verified per node from sysfs at probe time")
    elif name == "qwen":
        put("HEAD_IP", h)
        put("WORKER_IP", w)
        put("IFACE", link["head_if"])
        put("IB_HCA", f"={link['head_hca']}")
        put("IB_GID_INDEX", str(link["head_gid"]))
        if link["worker_if"] != link["head_if"]:
            put("WORKER_IFACE", link["worker_if"], activate=True, required=False)
        if link["worker_hca"] != link["head_hca"]:
            put("WORKER_IB_HCA", f"={link['worker_hca']}", activate=True, required=False)
    return out, notes


# ---------------------------------------------------------------------------
# merge engine
# ---------------------------------------------------------------------------


def merge_env(
    example_text: str,
    ours: Optional[Dict[str, str]],
    base: Optional[Dict[str, str]],
    fact_vals: Dict[str, Tuple[str, bool]],
    prefs: Dict[str, str],
    keep_removed: bool,
) -> Tuple[str, List[str]]:
    """Three-way merge of an upstream example against the deployed local env.

    Line-order preservation: the output is the example file's lines in their
    original order; only lines whose value must change are rewritten in place
    (keeping any trailing same-line comment and quote style), and local-only
    keys are appended after a marker at the very end. Comment blocks, blank
    lines and section layout are never reordered or dropped.
    """
    lines = parse_env(example_text)
    present = present_keys(lines)
    active_keys = set(active_map(lines))
    report: List[str] = []

    overrides: Dict[str, Tuple[str, str]] = {}
    for key, (value, _) in fact_vals.items():
        overrides[key] = (value, "fact")
    for key, value in prefs.items():
        if key in fact_vals:
            overrides[key] = (value, "pref>fact")
            report.append(f"[conflict] {key}: pref '{value}' overrides fact '{fact_vals[key][0]}'")
        else:
            overrides[key] = (value, "pref")

    reported: set = set()

    def report_once(entry: str) -> None:
        if entry not in reported:
            reported.add(entry)
            report.append(entry)

    commented_seen: set = set()
    for line in lines:
        if not line.key:
            continue
        key = line.key
        if line.active:
            if key in overrides:
                value, src = overrides[key]
                line.raw = line.render(value, True)
                report_once(f"[{src}] {key}={value}")
            elif ours is not None and key in ours:
                customized = base is None or key not in base or ours[key] != base[key]
                if customized:
                    if ours[key] != line.value:
                        line.raw = line.render(ours[key], True)
                        report_once(f"[kept] {key}={ours[key]} (customized, carried over)")
                elif ours[key] != line.value:
                    report_once(
                        f"[adopt] {key}: {base[key] if base else '?'} -> {line.value} (upstream default change, you had not customized)"
                    )
            elif ours is not None:
                report_once(f"[new] {key}={line.value} (new upstream key, default kept)")
        else:
            if key in active_keys:
                continue
            if key in commented_seen:
                continue
            commented_seen.add(key)
            if key in prefs:
                line.raw = line.render(prefs[key], True)
                report_once(f"[pref] {key}={prefs[key]} (activated from .env.local)")
            elif key in fact_vals and fact_vals[key][1]:
                value = fact_vals[key][0]
                line.raw = line.render(value, True)
                report_once(f"[fact] {key}={value} (activated)")
            elif ours is not None and key in ours:
                customized = base is None or key not in base or ours[key] != base[key]
                if customized:
                    line.raw = line.render(ours[key], True)
                    report_once(
                        f"[kept] {key}={ours[key]} (you had it active+customized; upstream now ships it commented)"
                    )
                else:
                    report_once(
                        f"[disabled] {key} (upstream commented it out; your value matched the old default)"
                    )

    if ours:
        for key in ours:
            if key not in present:
                if keep_removed:
                    report.append(f"[kept] {key}={ours[key]} (removed upstream, preserved)")
                else:
                    report.append(f"[dropped] {key}={ours[key]} (removed upstream; use --keep-removed to preserve)")
    for key in prefs:
        if key not in present:
            report.append(f"[pref] {key}={prefs[key]} (appended: not in upstream example)")
    for key in fact_vals:
        if key not in present:
            report.append(f"[skip] {key}: fact key not in this example")

    out = [line.raw for line in lines]
    extra: List[Tuple[str, str]] = []
    if keep_removed and ours:
        for key, value in ours.items():
            if key not in present:
                extra.append((key, value))
    for key, value in prefs.items():
        if key not in present:
            extra.append((key, value))
    if extra:
        out.append("")
        out.append("# --- env-tool: local-only keys (not in upstream example) ---")
        for key, value in extra:
            out.append(f"{key}={value}")

    result = "\n".join(out)
    if example_text.endswith("\n"):
        result += "\n"
    return result, report


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def discover_example(repo: Path) -> Path:
    tiers = [
        [repo / n for n in EXAMPLE_NAMES],
        sorted(repo.glob(".env.*.example")),
        sorted(repo.glob(".env.*.sample")),
    ]
    for tier in tiers:
        found = [p for p in tier if p.is_file()]
        if len(found) == 1:
            return found[0]
        if len(found) > 1:
            names = ", ".join(p.name for p in found)
            raise RuntimeError(f"multiple example files found: {names}; use --example")
    raise RuntimeError(
        f"no example env file found in {repo} "
        f"(looked for .env.example, .env.sample, .env.*.example, .env.*.sample)"
    )


def output_path(example: Path) -> Path:
    name = example.name
    if name.endswith((".example", ".sample")):
        return example.with_name(name.rsplit(".", 1)[0])
    return example.with_name(name + ".local")


def git_repo_root(repo: Path) -> Optional[Path]:
    try:
        proc = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--show-toplevel"],
            capture_output=True, text=True, timeout=15,
        )
        if proc.returncode == 0:
            return Path(proc.stdout.strip())
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def git_base_text(repo: Path, example: Path) -> Optional[str]:
    root = git_repo_root(repo)
    if root is None:
        return None
    rel = example.resolve().relative_to(root)
    try:
        proc = subprocess.run(
            ["git", "-C", str(root), "show", f"ORIG_HEAD:{rel}"],
            capture_output=True, text=True, timeout=15,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if proc.returncode != 0:
        return None
    return proc.stdout


def ensure_ignored(repo: Path, names: List[str]) -> None:
    root = git_repo_root(repo)
    if root is None:
        return
    exclude = root / ".git" / "info" / "exclude"
    existing = exclude.read_text().splitlines() if exclude.exists() else []
    changed = False
    for name in names:
        try:
            proc = subprocess.run(
                ["git", "-C", str(repo), "check-ignore", "-q", name],
                capture_output=True, timeout=10,
            )
            if proc.returncode == 0:
                continue
        except (OSError, subprocess.SubprocessError):
            pass
        if name not in existing:
            existing.append(name)
            changed = True
    if changed:
        exclude.parent.mkdir(parents=True, exist_ok=True)
        exclude.write_text("\n".join(existing) + "\n")


def cmd_apply(args: argparse.Namespace) -> int:
    implicit = args.repo is None
    repo = (args.repo if args.repo is not None else Path.cwd()).resolve()
    if not repo.is_dir():
        print(f"env-tool: error: not a directory: {repo}", file=sys.stderr)
        return 2
    home = Path.home().resolve()
    if repo == home or repo == Path("/").resolve():
        print(
            f"env-tool: error: refusing to operate on {repo}; "
            "cd into a recipe checkout or pass its path explicitly",
            file=sys.stderr,
        )
        return 2
    if implicit:
        root = git_repo_root(repo)
        if root is None:
            print(
                f"env-tool: error: {repo} is not a git repository; implicit apply "
                "(no path argument) requires a recipe checkout — pass an explicit "
                "path to override",
                file=sys.stderr,
            )
            return 2
        repo = root
    example = Path(args.example).resolve() if args.example else discover_example(repo)
    if not example.is_file():
        print(f"env-tool: error: example file not found: {example}", file=sys.stderr)
        return 2
    output = output_path(example)

    facts_path = config_dir() / "facts.json"
    facts = load_json(facts_path)
    if args.probe or facts is None:
        config = load_config()
        head = args.head or config.get("head")
        worker = args.worker or config.get("worker")
        if not head or not worker:
            print(
                "env-tool: error: no cached facts and no configured targets; "
                "run: env-tool probe --head <ssh-target> --worker <ssh-target>",
                file=sys.stderr,
            )
            return 2
        try:
            facts = build_facts(head, worker, config.get("primary_subnet"))
        except RuntimeError as exc:
            print(f"env-tool: error: probe failed: {exc}", file=sys.stderr)
            return 2
        save_json(facts_path, facts)

    example_text = example.read_text()
    lines = parse_env(example_text)
    adapter = args.adapter or detect_adapter(lines)
    if adapter == "plain":
        if implicit:
            print(
                f"env-tool: error: no known recipe signature in {example.name}; "
                "refusing implicit apply — this does not look like a supported "
                "recipe checkout (pass an explicit path or --adapter to force)",
                file=sys.stderr,
            )
            return 2
        print(
            "env-tool: warning: no known recipe signature detected; "
            "only preferences will be overlaid",
            file=sys.stderr,
        )
    fact_vals, notes = adapter_values(adapter, facts, present_keys(lines))

    prefs_path = Path(args.prefs).resolve() if args.prefs else repo / PREFS_NAME
    prefs = parse_kv(prefs_path.read_text()) if prefs_path.is_file() else {}

    ours = parse_kv(output.read_text()) if output.is_file() else None
    base_text = Path(args.base).read_text() if args.base else git_base_text(repo, example)
    base = parse_kv(base_text) if base_text else None

    new_text, report = merge_env(
        example_text, ours, base, fact_vals, prefs, args.keep_removed
    )

    print(f"repo:     {repo}")
    print(f"example:  {example.name} -> {output.name}")
    print(f"adapter:  {adapter}" + ("" if args.adapter else " (auto-detected)"))
    print(f"facts:    {facts['head']} <-> {facts['worker']}, {len(facts['links'])} link(s), probed {facts['probed_at']}")
    print(f"base:     {'git ORIG_HEAD' if base is not None else 'unavailable (customized values kept conservatively)'}")
    print(f"prefs:    {prefs_path}" + ("" if prefs else " (none)"))
    for note in notes:
        print(f"note:     {note}")
    for entry in report:
        print(f"  {entry}")

    if args.check:
        if output.is_file() and output.read_text() == new_text:
            print("check: up to date")
            return 0
        print("check: file would change")
        return 1

    if output.is_file():
        shutil.copy2(output, output.with_name(output.name + ".prev"))
    tmp = output.with_name(output.name + ".tmp")
    tmp.write_text(new_text)
    tmp.replace(output)
    print(f"wrote:    {output}" + (" (backup: .prev)" if ours else ""))
    ensure_ignored(repo, [PREFS_NAME, output.name])
    return 0


# ---------------------------------------------------------------------------
# probe / facts commands
# ---------------------------------------------------------------------------


def cmd_probe(args: argparse.Namespace) -> int:
    config = load_config()
    head = args.head or config.get("head")
    worker = args.worker or config.get("worker")
    if not head or not worker:
        print(
            "env-tool: error: --head and --worker required "
            "(or set them once via probe; they are cached in config.json)",
            file=sys.stderr,
        )
        return 2
    primary_subnet = args.primary_subnet or config.get("primary_subnet")
    try:
        facts = build_facts(head, worker, primary_subnet)
    except RuntimeError as exc:
        print(f"env-tool: error: {exc}", file=sys.stderr)
        return 2
    save_json(config_dir() / "facts.json", facts)
    save_json(
        config_dir() / "config.json",
        {
            "head": head,
            "worker": worker,
            "primary_subnet": primary_subnet,
        },
    )
    print_facts(facts)
    issues: List[str] = []
    if args.verify or args.verify_rdma:
        for link in facts["links"]:
            issues += verify_link(head, worker, link, rdma=args.verify_rdma)
        for issue in issues:
            print(f"verify:  {issue}")
        if not issues:
            print(f"verify:  {len(facts['links'])} link(s) passed"
                  + (" (incl. RDMA)" if args.verify_rdma else ""))
    return 1 if issues else 0


def cmd_facts(args: argparse.Namespace) -> int:
    facts = load_json(config_dir() / "facts.json")
    if facts is None:
        print("env-tool: error: no cached facts; run env-tool probe first", file=sys.stderr)
        return 2
    if args.json:
        print(json.dumps(facts, indent=2, sort_keys=True))
    else:
        print_facts(facts)
    return 0


# ---------------------------------------------------------------------------
# legacy merge command
# ---------------------------------------------------------------------------


def cmd_merge(args: argparse.Namespace) -> int:
    for label, path in (("example", args.example), ("local", args.local)):
        if not path.is_file():
            print(f"env-tool: error: {label} file not found: {path}", file=sys.stderr)
            return 1
    if args.output.resolve() == args.local.resolve():
        print(f"env-tool: error: output would overwrite the local file: {args.output}", file=sys.stderr)
        return 1
    local = parse_kv(args.local.read_text())
    out_lines = []
    for line in parse_env(args.example.read_text()):
        if line.active and line.key in local:
            out_lines.append(line.render(local[line.key], True))
        else:
            out_lines.append(line.raw)
    example_text = args.example.read_text()
    result = "\n".join(out_lines)
    if example_text.endswith("\n"):
        result += "\n"
    args.output.write_text(result)
    print(f"merged: {args.example} + {args.local} -> {args.output}")
    return 0


# ---------------------------------------------------------------------------
# cli
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="env-tool",
        description="Cluster-aware env file management for DGX Spark recipes.",
    )
    parser.add_argument("--version", action="version", version="env-tool 0.2.0")
    sub = parser.add_subparsers(dest="command")

    p_merge = sub.add_parser(
        "merge", help="legacy: overlay local values onto an example env file"
    )
    p_merge.add_argument("example", type=Path)
    p_merge.add_argument("local", type=Path)
    p_merge.add_argument("-o", "--output", type=Path, required=True)
    p_merge.set_defaults(func=cmd_merge)

    p_probe = sub.add_parser("probe", help="discover inter-node RoCE links over SSH")
    p_probe.add_argument("--head", help="ssh target for the head node (or localhost)")
    p_probe.add_argument("--worker", help="ssh target for the worker node")
    p_probe.add_argument("--primary-subnet", help="preferred primary link subnet (e.g. 192.168.100.0/24)")
    p_probe.add_argument("--verify", action="store_true", help="ping + jumbo checks per link")
    p_probe.add_argument("--verify-rdma", action="store_true", help="also run an ib_write_bw RDMA test per link")
    p_probe.set_defaults(func=cmd_probe)

    p_facts = sub.add_parser("facts", help="print cached cluster facts")
    p_facts.add_argument("--json", action="store_true")
    p_facts.set_defaults(func=cmd_facts)

    p_apply = sub.add_parser("apply", help="regenerate a recipe repo's local env file")
    p_apply.add_argument(
        "repo",
        type=Path,
        nargs="?",
        default=None,
        help="recipe repo (default: git checkout containing the current directory)",
    )
    p_apply.add_argument("--example", type=Path, help="override example file path")
    p_apply.add_argument("--prefs", type=Path, help=f"preferences file (default: {PREFS_NAME} in repo)")
    p_apply.add_argument("--adapter", choices=["dspark", "dspark-dual", "glm", "qwen", "plain"])
    p_apply.add_argument("--check", action="store_true", help="dry-run: exit 1 if the file would change")
    p_apply.add_argument("--keep-removed", action="store_true", help="preserve keys removed upstream")
    p_apply.add_argument("--probe", action="store_true", help="force a fresh probe before applying")
    p_apply.add_argument("--head", help="ssh target override for --probe")
    p_apply.add_argument("--worker", help="ssh target override for --probe")
    p_apply.add_argument("--base", type=Path, help="override pre-pull example (default: git ORIG_HEAD)")
    p_apply.set_defaults(func=cmd_apply)

    argv = sys.argv[1:]
    if argv and not argv[0].startswith("-") and argv[0] not in ("merge", "probe", "facts", "apply"):
        argv = ["merge"] + argv
    args = parser.parse_args(argv)
    if args.command is None:
        parser.print_help()
        sys.exit(2)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
