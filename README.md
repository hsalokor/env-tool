# env-tool

Cluster-aware env file management for a 2-node DGX Spark (GB10) cluster.

Upstream model recipes (`.env.example` / `.env.sample` / `.env.dspark.example`)
change constantly: new keys, new defaults, renamed knobs. Until now every
`git pull` meant manually re-merging your node-specific values — IPs, RoCE
interface names, HCA devices, GID indices — into the fresh example, in three
different key-naming schemes per recipe family. env-tool automates that:

1. **probe** — discovers the inter-node RoCE fabric by SSH: which netdevs
   form direct links between head and worker, their subnets and IPs, the
   matching RoCE HCAs, and the valid RoCE v2 GID index per node (read from
   sysfs, exactly the way NCCL's own resolver does it).
2. **adapters** — map those canonical facts onto each recipe family's key
   names, auto-detected from the example file's key signature.
3. **apply** — regenerates the recipe's local env from the fresh upstream
   example: fact keys filled from the probe, user preferences from a small
   `.env.local`, and everything else merged three-way against the pre-pull
   example (git `ORIG_HEAD`) so your customizations survive while untouched
   values adopt upstream's new defaults.

Comment blocks, blank lines and line order are preserved byte-for-byte: only
values change, in place. Local-only keys are appended at the end under a
marker, never interleaved.

## Installation (head host)

The tool is stdlib-only Python (>= 3.9). On the head node, clone and install
with [uv](https://docs.astral.sh/uv/):

```sh
git clone <this-repo> ~/src/env-tool
cd ~/src/env-tool
uv tool install .
```

This puts `env-tool` on your PATH (`~/.local/bin`). After pulling updates to
the clone, reinstall with `uv tool install --force .`.

`pip install .` (or just copying `env_tool.py` somewhere) works equally well —
there are no dependencies.

## First-run setup

Discover the cluster once, from the head node (where the worker is reachable
directly):

```sh
env-tool probe --head localhost --worker <worker-host>
```

`<worker-host>` is any ssh target your `~/.ssh/config` resolves for the
worker node. From another machine, pass full ssh targets for both nodes
(e.g. `env-tool probe --head <user>@<head-host> --worker <user>@<worker-host>`).
The targets are cached in
`~/.config/env-tool/config.json` so later probes need no arguments.

Optional flags:

- `--verify` — additionally ping (small + jumbo/DF) each discovered link
- `--verify-rdma` — also run an `ib_write_bw` RDMA test per link
- `--primary-subnet 192.168.100.0/24` — pin which link is primary when the
  cluster has several (default: lowest head IP)

Facts are cached in `~/.config/env-tool/facts.json` (override the directory
with `ENV_TOOL_CONFIG_DIR`). `env-tool facts` prints them; `env-tool facts
--json` dumps them. GID indices can drift after NIC/host reboots, so re-run
`probe` after hardware events (recipes with boot-time GID auto-resolution,
like the MiaAI-Lab DSpark one, are unaffected either way).

## Daily workflow

After upstream recipe changes:

```sh
cd ~/model-setups/<recipe>
git pull
env-tool apply --check   # dry-run: drift report, exit 1 if the file would change
env-tool apply           # write .env (previous copy kept as .env.prev)
```

`apply` also accepts an explicit path (`env-tool apply ~/model-setups/<recipe>`)
— useful from outside the checkout or in scripts.

## Guards (running without a path)

When `apply` is invoked with no path it operates on the git checkout
containing the current directory — and refuses to run in the wrong place:

- home (`~`) and `/` are always refused, even with an explicit path
- implicit invocation requires a git repository (otherwise: error)
- the checkout must contain a known example file
- the example must match a known recipe signature (`dspark`, `dspark-dual`,
  `glm`, `qwen`) — an unrelated project's `.env.example` is rejected

Pass an explicit path (and `--adapter`, if needed) to override the signature
guard; explicit non-home paths skip the git-repo requirement.

## What apply fills in

| Adapter | Example file | Output | Recipe family |
|---|---|---|---|
| `dspark` | `.env.dspark.example` | `.env.dspark` | MiaAI-Lab DeepSeek-V4-Flash-DSpark (GID left to its boot-time auto-resolver) |
| `dspark-dual` | `.env.dspark.example` | `.env.dspark` | dual-HCA forks (comma HCA list, `NCCL_IB_MERGE_NICS`, pinned GID) |
| `glm` | `.env.example` | `.env` | GLM-5.3-Flash-EXL3 (per-node `HEAD_/WORKER_CX7_*`, GID preflight) |
| `qwen` | `.env.sample` | `.env` | Qwen3.8-Flash-Next (`IFACE`, `IB_HCA="=..."` exact-match syntax, quoted values) |

Typical fact keys: `WORKER_HOST` / `MASTER_ADDR` / `HEAD_IP` / `WORKER_IP`,
`NCCL_IB_HCA`, `NCCL_/TP_/GLOO_SOCKET_IFNAME`, `VLLM_HOST_IP` /
`WORKER_VLLM_HOST_IP`, `HEAD_/WORKER_CX7_IF|IB`, `IFACE`, `IB_HCA`,
`NCCL_IB_GID_INDEX` (or per-rank `HEAD_GID`/`WORKER_GID` when nodes differ).
Quote style and trailing same-line comments of replaced lines are preserved.

### Three-way merge semantics

With the pre-pull example available from git (`ORIG_HEAD`, automatic after a
`git pull`; override with `--base FILE`):

- values you customized (differ from the pre-pull example) are carried over
- values you never touched adopt upstream's new defaults
- keys you activated that upstream now ships commented stay active (yours)
- keys upstream comments out that you never customized follow upstream
- keys removed upstream are reported; `--keep-removed` preserves them in the
  appended local-only section
- new upstream keys keep their defaults and are reported

Report tags: `[fact]` machine-derived, `[pref]` from `.env.local`,
`[kept]` your customization carried, `[adopt]` new upstream default taken,
`[new]` new upstream key, `[disabled]` upstream disabled an untouched key,
`[dropped]` key gone upstream, `[conflict]` pref overrode a fact.

## User preferences: `.env.local`

Machine-derivable facts never live in files — they are re-derived by `probe`.
Everything you choose by hand goes in a per-repo `.env.local` (plain
`KEY=VALUE`, git-ignored; env-tool adds it to `.git/info/exclude` for you):

```sh
# ~/model-setups/DeepSeek-v4-Flash-DSpark-2x-DGX-Spark/.env.local
ABLITERATED=1
DEFAULT_THINKING=max
```

Preferences override facts (reported as `[conflict]`), activate commented-out
recipe keys (e.g. `VLLM_API_KEY=...`), and keys unknown to the recipe are
appended at the end. Use `--prefs FILE` for a different location.

## Legacy merge

The original value-overlay behaviour is kept as a subcommand (and as the
default when the first argument is a path):

```sh
env-tool merge EXAMPLE LOCAL -o OUTPUT
env-tool EXAMPLE LOCAL -o OUTPUT
```

## Development

```sh
python3 -m unittest discover -s tests
```
