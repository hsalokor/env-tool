import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from env_tool import (
    EnvLine,
    active_map,
    adapter_values,
    build_facts,
    cmd_apply,
    detect_adapter,
    discover_example,
    main,
    merge_env,
    output_path,
    parse_env,
    parse_kv,
    parse_node_output,
    present_keys,
    split_value,
    deduce_links,
)

REPO = Path(__file__).resolve().parent.parent

HEAD_OUT = """===IFACES4===
2: enP7s7    inet 192.168.1.182/24 brd 192.168.1.255 scope global enP7s7\\       valid_lft forever
3: enp1s0f0np0    inet 192.168.100.10/24 brd 192.168.100.255 scope global noprefixroute enp1s0f0np0\\       valid_lft forever
5: enP2p1s0f0np0    inet 192.168.101.10/24 brd 192.168.101.255 scope global enP2p1s0f0np0\\       valid_lft forever
1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever
===LINKS===
1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN mode DEFAULT group default qlen 1000
2: enP7s7: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc mq state UP mode DEFAULT group default qlen 1000
3: enp1s0f0np0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 9000 qdisc mq state UP mode DEFAULT group default qlen 1000
4: enp1s0f1np1: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN mode DEFAULT group default qlen 1000
5: enP2p1s0f0np0: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 9000 qdisc mq state UP mode DEFAULT group default qlen 1000
6: enP2p1s1np1: <NO-CARRIER,BROADCAST,MULTICAST,UP> mtu 1500 qdisc mq state DOWN mode DEFAULT group default qlen 1000
===IBDEV2NETDEV===
rocep1s0f0 port 1 ==> enp1s0f0np0 (Up)
rocep1s0f1 port 1 ==> enp1s0f1np1 (Down)
roceP2p1s0f0 port 1 ==> enP2p1s0f0np0 (Up)
roceP2p1s1 port 1 ==> enP2p1s1np1 (Down)
===GIDS===
GID rocep1s0f0 1 0 IB/RoCE v1 fe80:0000:0000:0000:4ebb:47ff:fe82:456e
GID rocep1s0f0 1 1 RoCE v2 fe80:0000:0000:0000:4ebb:47ff:fe82:456e
GID rocep1s0f0 1 2 IB/RoCE v1 0000:0000:0000:0000:0000:ffff:c0a8:640a
GID rocep1s0f0 1 3 RoCE v2 0000:0000:0000:0000:0000:ffff:c0a8:640a
GID rocep1s0f0 1 4 RoCE v2 0000:0000:0000:0000:0000:0000:0000:0000
GID roceP2p1s0f0 1 3 RoCE v2 0000:0000:0000:0000:0000:ffff:c0a8:650a
GID roceP2p1s0f1 1 3 RoCE v2 0000:0000:0000:0000:0000:0000:0000:0000
===DONE==="
"""

WORKER_OUT = HEAD_OUT.replace("192.168.1.182", "192.168.1.186").replace(
    "192.168.100.10", "192.168.100.11"
).replace("192.168.101.10", "192.168.101.11").replace(
    "fe82:456e", "fee8:e004"
).replace(
    "c0a8:640a", "c0a8:640b"
).replace(
    "c0a8:650a", "c0a8:650b"
)

GLM_EXAMPLE = """# GLM
HEAD_IP=10.0.0.1
WORKER_IP=10.0.0.2
# WORKER_USER=
HEAD_CX7_IF=enp1s0f1np1
WORKER_CX7_IF=enp1s0f0np0
HEAD_CX7_IB=rocep1s0f1
WORKER_CX7_IB=rocep1s0f0
# NCCL_IB_GID_INDEX=2
# HEAD_GID=4
# WORKER_GID=3
PORT=8888
NCCL_IB_GID_INDEX=3
"""

QWEN_EXAMPLE = """# Qwen
HEAD_IP="10.0.0.1"
WORKER_IP="10.0.0.2"                          # SSH user on worker
IFACE="enp1s0f0np0"
IB_HCA="=rocep1s0f0"                   # Single device, exact match (leading =)
IB_GID_INDEX=3
#   NCCL_IB_HCA==rocep1s0f0,roceP2p1s0f0       # map names: ibdev2netdev
# HF_HOME="/data/hf"
# VLLM_API_KEY=sk-xxx
MAX_MODEL_LEN=1000000
"""

DSPARK_EXAMPLE = """# DeepSeek
WORKER_HOST=worker-host-or-roce-ip
MASTER_ADDR=head-roce-ip
NCCL_IB_HCA=rocepXsYfZ
NCCL_SOCKET_IFNAME=enpXsYfZnpN
TP_SOCKET_IFNAME=enp1s0f1np1
GLOO_SOCKET_IFNAME=enp1s0f1np1
# WORKER_NCCL_IB_HCA=rocep1s0f0
# NCCL_IB_GID_INDEX=3
VLLM_HOST_IP=head-roce-ip
WORKER_VLLM_HOST_IP=worker-roce-ip
ABLITERATED=0
MAX_NUM_BATCHED_TOKENS=8192
GPU_MEMORY_UTILIZATION_TEXT=0.83
"""

DUAL_EXAMPLE = """# dual-HCA fork
WORKER_HOST=worker-host-or-roce-ip
MASTER_ADDR=head-roce-ip
NCCL_IB_HCA=rocepXsYfZ,rocePWpXsYfZ
NCCL_SOCKET_IFNAME=enpXsYfZnpN,enPWpXsYfZnpN
NCCL_IB_MERGE_NICS=1
NCCL_IB_GID_INDEX=3
VLLM_HOST_IP=head-roce-ip
WORKER_VLLM_HOST_IP=worker-roce-ip
"""

FACTS = {
    "probed_at": "2026-09-01T12:00:00+00:00",
    "head": "head-node",
    "worker": "worker-node",
    "primary": 0,
    "links": [
        {
            "subnet": "192.168.100.0/24",
            "head_if": "enp1s0f0np0",
            "head_ip": "192.168.100.10",
            "head_hca": "rocep1s0f0",
            "head_gid": 3,
            "worker_if": "enp1s0f0np0",
            "worker_ip": "192.168.100.11",
            "worker_hca": "rocep1s0f0",
            "worker_gid": 3,
            "mtu": 9000,
        },
        {
            "subnet": "192.168.101.0/24",
            "head_if": "enP2p1s0f0np0",
            "head_ip": "192.168.101.10",
            "head_hca": "roceP2p1s0f0",
            "head_gid": 3,
            "worker_if": "enP2p1s0f0np0",
            "worker_ip": "192.168.101.11",
            "worker_hca": "roceP2p1s0f0",
            "worker_gid": 3,
            "mtu": 9000,
        },
    ],
    "notes": [],
}


class TestParsing(unittest.TestCase):
    def test_split_value_quotes_and_comment(self):
        self.assertEqual(split_value('"10.0.0.2"   # SSH user'), ("10.0.0.2", '"', "   # SSH user"))
        self.assertEqual(split_value('"=rocep1s0f0"  # exact'), ("=rocep1s0f0", '"', "  # exact"))
        self.assertEqual(split_value("0.835                # measured"), ("0.835", "", "                # measured"))
        self.assertEqual(split_value("plain"), ("plain", "", ""))
        self.assertEqual(split_value("'{}'"), ("{}", "'", ""))

    def test_commented_and_doc_lines(self):
        lines = parse_env(
            "# NCCL_IB_GID_INDEX=2\n"
            "#   NCCL_IB_HCA==rocep1s0f0,roceP2p1s0f0    # doc illustration\n"
            "# Set DSPARK_REVISION= (empty) to follow tip\n"
            "KEY=val\n"
        )
        self.assertTrue(lines[0].key == "NCCL_IB_GID_INDEX" and not lines[0].active)
        self.assertEqual(lines[1].key, "")
        self.assertEqual(lines[2].key, "")
        self.assertTrue(lines[3].active and lines[3].key == "KEY")

    def test_active_map_and_present(self):
        lines = parse_env(GLM_EXAMPLE)
        active = active_map(lines)
        present = present_keys(lines)
        self.assertEqual(active["HEAD_IP"], "10.0.0.1")
        self.assertIn("WORKER_USER", present)
        self.assertFalse(present["WORKER_USER"])
        self.assertTrue(present["HEAD_IP"])


class TestDeduce(unittest.TestCase):
    def test_two_links_with_gids(self):
        head = parse_node_output(HEAD_OUT)
        worker = parse_node_output(WORKER_OUT)
        links, notes = deduce_links(head, worker)
        self.assertEqual(len(links), 2)
        self.assertEqual(links[0]["head_ip"], "192.168.100.10")
        self.assertEqual(links[0]["head_gid"], 3)
        self.assertEqual(links[0]["worker_gid"], 3)
        self.assertEqual(links[0]["mtu"], 9000)
        self.assertEqual(links[1]["head_hca"], "roceP2p1s0f0")
        self.assertNotIn("enp1s0f1np1", [l["head_if"] for l in links])
        self.assertNotIn("enP7s7", [l["head_if"] for l in links])


class TestAdapters(unittest.TestCase):
    def test_detection(self):
        self.assertEqual(detect_adapter(parse_env(GLM_EXAMPLE)), "glm")
        self.assertEqual(detect_adapter(parse_env(QWEN_EXAMPLE)), "qwen")
        self.assertEqual(detect_adapter(parse_env(DSPARK_EXAMPLE)), "dspark")
        self.assertEqual(detect_adapter(parse_env(DUAL_EXAMPLE)), "dspark-dual")
        self.assertEqual(detect_adapter(parse_env("PORT=1\n")), "plain")

    def test_dspark_values(self):
        vals, notes = adapter_values("dspark", FACTS, present_keys(parse_env(DSPARK_EXAMPLE)))
        self.assertEqual(vals["WORKER_HOST"], ("192.168.100.11", False))
        self.assertEqual(vals["NCCL_SOCKET_IFNAME"], ("enp1s0f0np0", False))
        self.assertEqual(vals["TP_SOCKET_IFNAME"], ("enp1s0f0np0", False))
        self.assertNotIn("NCCL_IB_GID_INDEX", vals)
        self.assertTrue(any("auto-resolver" in n for n in notes))

    def test_dspark_dual_values(self):
        vals, _ = adapter_values("dspark-dual", FACTS, present_keys(parse_env(DUAL_EXAMPLE)))
        self.assertEqual(vals["NCCL_IB_HCA"], ("rocep1s0f0,roceP2p1s0f0", False))
        self.assertEqual(vals["NCCL_SOCKET_IFNAME"], ("enp1s0f0np0,enP2p1s0f0np0", False))
        self.assertEqual(vals["NCCL_IB_GID_INDEX"], ("3", False))

    def test_glm_values(self):
        vals, _ = adapter_values("glm", FACTS, present_keys(parse_env(GLM_EXAMPLE)))
        self.assertEqual(vals["HEAD_CX7_IF"], ("enp1s0f0np0", False))
        self.assertEqual(vals["HEAD_CX7_IB"], ("rocep1s0f0", False))
        self.assertEqual(vals["WORKER_CX7_IF"], ("enp1s0f0np0", False))
        self.assertEqual(vals["NCCL_IB_GID_INDEX"], ("3", False))

    def test_glm_per_rank_gid(self):
        facts = json.loads(json.dumps(FACTS))
        facts["links"][0]["worker_gid"] = 4
        vals, _ = adapter_values("glm", facts, present_keys(parse_env(GLM_EXAMPLE)))
        self.assertNotIn("NCCL_IB_GID_INDEX", vals)
        self.assertEqual(vals["HEAD_GID"], ("3", True))
        self.assertEqual(vals["WORKER_GID"], ("4", True))

    def test_qwen_values(self):
        vals, _ = adapter_values("qwen", FACTS, present_keys(parse_env(QWEN_EXAMPLE)))
        self.assertEqual(vals["IB_HCA"], ("=rocep1s0f0", False))
        self.assertEqual(vals["IFACE"], ("enp1s0f0np0", False))
        self.assertNotIn("WORKER_IFACE", vals)


class TestMerge(unittest.TestCase):
    def test_comment_block_and_order_preserved(self):
        example = (
            "# ===== header block =====\n"
            "# line two of the block\n"
            "\n"
            "A=1\n"
            "# doc illustration of an alternative:\n"
            "#   A=9       # illustration only\n"
            "# B=2\n"
            "C=3\n"
            "# trailing block\n"
            "# mentioning A=1 again in prose\n"
        )
        fact_vals = {"A": ("7", False)}
        text, _ = merge_env(example, None, None, fact_vals, {}, False)
        self.assertEqual(
            text,
            "# ===== header block =====\n"
            "# line two of the block\n"
            "\n"
            "A=7\n"
            "# doc illustration of an alternative:\n"
            "#   A=9       # illustration only\n"
            "# B=2\n"
            "C=3\n"
            "# trailing block\n"
            "# mentioning A=1 again in prose\n",
        )

    def test_active_line_rewritten_when_key_also_commented_earlier(self):
        example = (
            "# nodes section\n"
            "# NCCL_IB_GID_INDEX=2\n"
            "# HEAD_GID=4\n"
            "\n"
            "# bottom section\n"
            "NCCL_IB_GID_INDEX=3\n"
        )
        fact_vals = {"NCCL_IB_GID_INDEX": ("5", False)}
        text, report = merge_env(example, None, None, fact_vals, {}, False)
        self.assertIn("NCCL_IB_GID_INDEX=5", text)
        self.assertIn("# NCCL_IB_GID_INDEX=2", text)
        lines = [l for l in text.splitlines() if l.startswith("NCCL")]
        self.assertEqual(len(lines), 1)
        self.assertTrue(any("[fact] NCCL_IB_GID_INDEX=5" in r for r in report))

    def test_doc_block_alternatives_untouched(self):
        example = (
            "MAX_NUM_BATCHED_TOKENS=8192\n"
            "# perf data:\n"
            "# MAX_NUM_BATCHED_TOKENS=12288\n"
            "# MAX_NUM_BATCHED_TOKENS=12288\n"
        )
        ours = {"MAX_NUM_BATCHED_TOKENS": "2048"}
        base = {"MAX_NUM_BATCHED_TOKENS": "8192"}
        text, report = merge_env(example, ours, base, {}, {}, False)
        self.assertIn("MAX_NUM_BATCHED_TOKENS=2048", text)
        self.assertEqual(text.count("# MAX_NUM_BATCHED_TOKENS=12288"), 2)
        self.assertEqual(len([r for r in report if "MAX_NUM" in r]), 1)

    def test_fact_and_pref_overlay(self):
        fact_vals = {"WORKER_HOST": ("192.168.100.11", False)}
        prefs = {"ABLITERATED": "1"}
        text, report = merge_env(DSPARK_EXAMPLE, None, None, fact_vals, prefs, False)
        self.assertIn("WORKER_HOST=192.168.100.11", text)
        self.assertIn("ABLITERATED=1", text)
        self.assertTrue(any("[fact] WORKER_HOST" in r for r in report))
        self.assertTrue(any("[pref] ABLITERATED" in r for r in report))

    def test_three_way_customized_kept_default_adopted(self):
        base = parse_kv(DSPARK_EXAMPLE)
        ours = dict(base)
        ours["MAX_NUM_BATCHED_TOKENS"] = "2048"
        theirs = DSPARK_EXAMPLE.replace(
            "GPU_MEMORY_UTILIZATION_TEXT=0.83", "GPU_MEMORY_UTILIZATION_TEXT=0.835"
        ).replace("MAX_NUM_BATCHED_TOKENS=8192", "MAX_NUM_BATCHED_TOKENS=7168")
        text, report = merge_env(theirs, ours, base, {}, {}, False)
        self.assertIn("MAX_NUM_BATCHED_TOKENS=2048", text)
        self.assertIn("GPU_MEMORY_UTILIZATION_TEXT=0.835", text)
        self.assertTrue(any("[kept] MAX_NUM_BATCHED_TOKENS=2048" in r for r in report))
        self.assertTrue(any("[adopt] GPU_MEMORY_UTILIZATION_TEXT" in r for r in report))

    def test_user_activated_key_survives_upstream_commenting(self):
        base = {"NCCL_IB_GID_INDEX": "3"}
        ours = {"NCCL_IB_GID_INDEX": "2"}
        theirs = "# NCCL_IB_GID_INDEX=3\n"
        text, report = merge_env(theirs, ours, base, {}, {}, False)
        self.assertIn("NCCL_IB_GID_INDEX=2", text)
        self.assertNotIn("# NCCL_IB_GID_INDEX=2", text)
        self.assertTrue(any("[kept] NCCL_IB_GID_INDEX=2" in r for r in report))

    def test_upstream_disable_of_untouched_key_followed(self):
        base = {"VLLM_API_KEY": "sk-abc"}
        ours = {"VLLM_API_KEY": "sk-abc"}
        theirs = "# VLLM_API_KEY=sk-abc\n"
        text, report = merge_env(theirs, ours, base, {}, {}, False)
        self.assertIn("# VLLM_API_KEY=sk-abc", text)
        self.assertTrue(any("disabled upstream" in r or "upstream" in r for r in report))

    def test_pref_activates_commented_key_with_quotes(self):
        prefs = {"HF_HOME": "/data/hf"}
        text, report = merge_env(QWEN_EXAMPLE, None, None, {}, prefs, False)
        self.assertIn('HF_HOME="/data/hf"', text)
        self.assertTrue(any("[pref] HF_HOME" in r for r in report))

    def test_quote_style_preserved_on_replace(self):
        fact_vals = {"HEAD_IP": ("192.168.100.10", False)}
        text, _ = merge_env(QWEN_EXAMPLE, None, None, fact_vals, {}, False)
        self.assertIn('HEAD_IP="192.168.100.10"', text)

    def test_trailing_comment_preserved_on_replace(self):
        fact_vals = {"WORKER_IP": ("192.168.100.11", False)}
        text, _ = merge_env(QWEN_EXAMPLE, None, None, fact_vals, {}, False)
        self.assertIn('# SSH user on worker', text.split("WORKER_IP=")[1].splitlines()[0])

    def test_dropped_key_warning_and_keep(self):
        ours = {"OLD_KEY": "x"}
        theirs = "KEY=val\n"
        _, report = merge_env(theirs, ours, None, {}, {}, False)
        self.assertTrue(any("[dropped] OLD_KEY" in r for r in report))
        text, _ = merge_env(theirs, ours, None, {}, {}, True)
        self.assertIn("OLD_KEY=x", text)

    def test_pref_unknown_key_appended(self):
        text, report = merge_env(DSPARK_EXAMPLE, None, None, {}, {"MY_KNOB": "7"}, False)
        self.assertIn("MY_KNOB=7", text)
        self.assertIn("env-tool: local-only keys", text)
        self.assertTrue(any("appended" in r for r in report))

    def test_conflict_reported(self):
        fact_vals = {"ABLITERATED": ("0", False)}
        text, report = merge_env(DSPARK_EXAMPLE, None, None, fact_vals, {"ABLITERATED": "1"}, False)
        self.assertIn("ABLITERATED=1", text)
        self.assertTrue(any("[conflict] ABLITERATED" in r for r in report))


class TestApplyCLI(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.config = self.dir / "config"
        os.environ["ENV_TOOL_CONFIG_DIR"] = str(self.config)
        self.config.mkdir()
        (self.config / "facts.json").write_text(json.dumps(FACTS))

    def run_apply(self, repo, *extra):
        argv = ["apply"] + ([str(repo)] if repo is not None else []) + list(extra)
        old_argv = sys.argv
        old_cwd = os.getcwd()
        sys.argv = ["env-tool"] + argv
        try:
            with self.assertRaises(SystemExit) as cm:
                main()
            return cm.exception.code
        finally:
            sys.argv = old_argv
            os.chdir(old_cwd)

    def test_discover_and_output_names(self):
        repo = self.dir / "glm"
        repo.mkdir()
        (repo / ".env.example").write_text(GLM_EXAMPLE)
        example = discover_example(repo)
        self.assertEqual(example.name, ".env.example")
        self.assertEqual(output_path(example).name, ".env")
        repo2 = self.dir / "ds"
        repo2.mkdir()
        (repo2 / ".env.dspark.example").write_text(DSPARK_EXAMPLE)
        self.assertEqual(output_path(discover_example(repo2)).name, ".env.dspark")
        repo3 = self.dir / "qwen"
        repo3.mkdir()
        (repo3 / ".env.sample").write_text(QWEN_EXAMPLE)
        self.assertEqual(output_path(discover_example(repo3)).name, ".env")

    def test_apply_first_time(self):
        repo = self.dir / "ds"
        repo.mkdir()
        (repo / ".env.dspark.example").write_text(DSPARK_EXAMPLE)
        (repo / ".env.local").write_text("ABLITERATED=1\n")
        rc = self.run_apply(repo)
        self.assertEqual(rc, 0)
        out = (repo / ".env.dspark").read_text()
        self.assertIn("WORKER_HOST=192.168.100.11", out)
        self.assertIn("MASTER_ADDR=192.168.100.10", out)
        self.assertIn("NCCL_SOCKET_IFNAME=enp1s0f0np0", out)
        self.assertIn("TP_SOCKET_IFNAME=enp1s0f0np0", out)
        self.assertIn("GLOO_SOCKET_IFNAME=enp1s0f0np0", out)
        self.assertIn("VLLM_HOST_IP=192.168.100.10", out)
        self.assertIn("ABLITERATED=1", out)

    def test_apply_check_detects_change_and_idempotence(self):
        repo = self.dir / "ds"
        repo.mkdir()
        (repo / ".env.dspark.example").write_text(DSPARK_EXAMPLE)
        rc = self.run_apply(repo, "--check")
        self.assertEqual(rc, 1)
        self.run_apply(repo)
        rc = self.run_apply(repo, "--check")
        self.assertEqual(rc, 0)

    def test_apply_with_git_base(self):
        repo = self.dir / "ds"
        repo.mkdir()
        (repo / ".env.dspark.example").write_text(DSPARK_EXAMPLE)
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        subprocess.run(["git", "-C", str(repo), "add", ".env.dspark.example"], check=True)
        subprocess.run(
            ["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "base"],
            check=True,
        )
        base_sha = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        deployed = DSPARK_EXAMPLE.replace(
            "WORKER_HOST=worker-host-or-roce-ip", "WORKER_HOST=192.168.100.11"
        ).replace(
            "MAX_NUM_BATCHED_TOKENS=8192", "MAX_NUM_BATCHED_TOKENS=2048"
        )
        (repo / ".env.dspark").write_text(deployed)
        (repo / ".env.dspark.example").write_text(
            DSPARK_EXAMPLE.replace(
                "MAX_NUM_BATCHED_TOKENS=8192", "MAX_NUM_BATCHED_TOKENS=7168"
            ).replace(
                "GPU_MEMORY_UTILIZATION_TEXT=0.83", "GPU_MEMORY_UTILIZATION_TEXT=0.835"
            )
        )
        subprocess.run(["git", "-C", str(repo), "update-ref", "ORIG_HEAD", base_sha], check=True)
        self.run_apply(repo)
        out = (repo / ".env.dspark").read_text()
        self.assertIn("MAX_NUM_BATCHED_TOKENS=2048", out)
        self.assertIn("GPU_MEMORY_UTILIZATION_TEXT=0.835", out)
        self.assertIn("WORKER_HOST=192.168.100.11", out)
        self.assertTrue((repo / ".env.dspark.prev").is_file())

    def test_git_exclude_housekeeping(self):
        repo = self.dir / "glm"
        repo.mkdir()
        (repo / ".env.example").write_text(GLM_EXAMPLE)
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        (repo / ".env.local").write_text("PORT=9999\n")
        self.run_apply(repo)
        exclude = (repo / ".git" / "info" / "exclude").read_text()
        self.assertIn(".env.local", exclude)
        self.assertIn(".env", exclude.splitlines())
        rc = self.run_apply(repo, "--check")
        self.assertEqual(rc, 0)

    def test_implicit_apply_uses_checkout_root(self):
        repo = self.dir / "glm2"
        repo.mkdir()
        (repo / ".env.example").write_text(GLM_EXAMPLE)
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        sub = repo / "scripts"
        sub.mkdir()
        os.chdir(sub)
        rc = self.run_apply(None)
        self.assertEqual(rc, 0)
        self.assertTrue((repo / ".env").is_file())
        self.assertIn("HEAD_IP=192.168.100.10", (repo / ".env").read_text())

    def test_implicit_refuses_non_git_dir(self):
        repo = self.dir / "nogit"
        repo.mkdir()
        (repo / ".env.example").write_text(GLM_EXAMPLE)
        os.chdir(repo)
        rc = self.run_apply(None)
        self.assertEqual(rc, 2)
        self.assertFalse((repo / ".env").exists())

    def test_implicit_refuses_home(self):
        fakehome = self.dir / "fakehome"
        fakehome.mkdir()
        (fakehome / ".env.example").write_text(GLM_EXAMPLE)
        subprocess.run(["git", "init", "-q", str(fakehome)], check=True)
        old_home = os.environ.get("HOME")
        os.environ["HOME"] = str(fakehome)
        try:
            os.chdir(fakehome)
            rc = self.run_apply(None)
            self.assertEqual(rc, 2)
            self.assertFalse((fakehome / ".env").exists())
        finally:
            if old_home is not None:
                os.environ["HOME"] = old_home

    def test_implicit_refuses_unknown_signature(self):
        repo = self.dir / "plainrepo"
        repo.mkdir()
        (repo / ".env.example").write_text("SOMETHING_ELSE=1\n")
        subprocess.run(["git", "init", "-q", str(repo)], check=True)
        os.chdir(repo)
        rc = self.run_apply(None)
        self.assertEqual(rc, 2)
        self.assertFalse((repo / ".env").exists())
        rc = self.run_apply(repo)
        self.assertEqual(rc, 0)
        self.assertTrue((repo / ".env").exists())


class TestLegacyMerge(unittest.TestCase):
    def test_samples_still_merge(self):
        from env_tool import main as env_main
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        out = Path(tmp.name) / "out"
        old_argv = sys.argv
        sys.argv = [
            "env-tool", "merge",
            str(REPO / "samples" / "env.example"),
            str(REPO / "samples" / "env.local"),
            "-o", str(out),
        ]
        try:
            with self.assertRaises(SystemExit) as cm:
                env_main()
            self.assertEqual(cm.exception.code, 0)
        finally:
            sys.argv = old_argv
        text = out.read_text()
        self.assertIn("WORKER_HOST=192.168.100.11", text)
        self.assertIn("MASTER_ADDR=192.168.100.10", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
