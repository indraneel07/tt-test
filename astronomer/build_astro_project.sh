#!/usr/bin/env bash
# Prerequisites: Docker Desktop should be running, and the Astro CLI (astro) should be installed.
# Usage (from astronomer/):
#   ./build_astro_project.sh -e dev            # build project only
#   ./build_astro_project.sh -e dev --start    # build project then astro dev start
#   ./build_astro_project.sh --stop            # cd astro_project && astro dev stop; rm -rf astro_project
#   ./build_astro_project.sh --kill            # cd astro_project && astro dev kill; rm -rf astro_project
#   ./build_astro_project.sh -e dev --restart  # stop & delete if exists, rebuild, then start

set -euo pipefail

ENVIRONMENT=""
OUT_DIR="astro_project"
DO_START=0
DO_STOP=0
DO_KILL=0
DO_RESTART=0

usage(){ sed -n '1,200p' "$0"; }
log(){ printf "\033[1;34m[astro]\033[0m %s\n" "$*"; }
warn(){ printf "\033[1;33m[warn]\033[0m %s\n" "$*"; }
err(){ printf "\033[1;31m[err]\033[0m %s\n" "$*"; }
die(){ err "$1"; exit 1; }
have(){ command -v "$1" >/dev/null 2>&1; }

while [ $# -gt 0 ]; do
  case "$1" in
    -e|--env) ENVIRONMENT="${2:-}"; shift 2;;
    -o|--out) OUT_DIR="${2:-}"; shift 2;;
    --start)  DO_START=1; shift;;
    --stop)   DO_STOP=1; shift;;
    --kill)   DO_KILL=1; shift;;
    --restart) DO_RESTART=1; shift;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; echo; usage; exit 1;;
  esac
done
[ $((DO_STOP+DO_KILL+DO_RESTART)) -le 1 ] || die "Use only one of --stop, --kill, or --restart."

case "$OUT_DIR" in ""|"/"|"."|"./"| "dags"|"include"|"plugins") die "Unsafe OUT_DIR='$OUT_DIR'";; esac

ROOT="$(pwd -P)"
OUT_DIR_ABS="$ROOT/$OUT_DIR"

ensure_python(){
  if have python; then PY_CMD="python"
  elif have python3; then PY_CMD="python3"
  elif have py; then PY_CMD="py -3"
  else die "Python not found in PATH. Please install Python 3."; fi
  if ! eval "$PY_CMD -m pip --version" >/dev/null 2>&1; then
    log "Bootstrapping pip with ensurepip for this Python..."
    eval "$PY_CMD -m ensurepip --upgrade" >/dev/null 2>&1 || true
  fi
  eval "$PY_CMD -m pip --version" >/dev/null 2>&1 || die "pip still not available for ($PY_CMD)."
  if ! eval "$PY_CMD -c \"import yaml\"" >/dev/null 2>&1; then
    log "Installing PyYAML via pip (--user)…"
    eval "$PY_CMD -m pip install --user PyYAML" >/dev/null
  fi
}
py(){ eval "$PY_CMD" "$@"; }

py_glob_abs(){
  local base_abs="$1" pat="$2"
  py - "$base_abs" "$pat" <<'PY'
import sys, os, glob
base = os.path.abspath(sys.argv[1])
pat  = sys.argv[2]
for p in glob.glob(os.path.join(base, pat), recursive=True):
    p = os.path.abspath(p)
    try:
        if os.path.commonpath([p, base]) != base:
            continue
    except ValueError:
        continue
    print(p.replace("\\","/"))
PY
}

py_rel_abs(){
  local base_abs="$1" path_abs="$2"
  py - "$base_abs" "$path_abs" <<'PY'
import sys, os
base = os.path.abspath(sys.argv[1])
p    = os.path.abspath(sys.argv[2])
print(os.path.relpath(p, start=base).replace("\\","/"))
PY
}

py_list(){
  local key="$1"
  py - "$CFG_FILE" "$key" <<'PY'
import sys, yaml
cfg_path, key = sys.argv[1], sys.argv[2]
try:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
except Exception as e:
    print(f"[YAML error] {cfg_path}: {e}", file=sys.stderr); sys.exit(2)
for v in (cfg.get(key) or []):
    print(v)
PY
}

py_scalar(){
  local key="$1"
  py - "$CFG_FILE" "$key" <<'PY'
import sys, yaml
cfg_path, key = sys.argv[1], sys.argv[2]
try:
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
except Exception as e:
    print(f"[YAML error] {cfg_path}: {e}", file=sys.stderr); sys.exit(2)
print(((cfg.get("other_files") or {}).get(key) or "").strip())
PY
}

py_copy_dir_filtered(){
  local src_abs="$1" dest_abs="$2"
  py - "$src_abs" "$dest_abs" <<'PY'
import sys, os, shutil
src = os.path.abspath(sys.argv[1])
dst = os.path.abspath(sys.argv[2])

IGNORE_DIRS = {"__pycache__", ".pytest_cache", ".ipynb_checkpoints", ".git", ".hg", ".svn", ".idea", ".vscode", "venv", ".venv"}
IGNORE_FILE_NAMES = {".DS_Store", "Thumbs.db"}
IGNORE_FILE_SUFFIXES = (".pyc", ".pyo", ".pyd")

for root, dirs, files in os.walk(src):
    # prune ignored directories
    dirs[:] = [d for d in dirs if d not in IGNORE_DIRS]
    rel = os.path.relpath(root, src)
    rel = "" if rel == "." else rel
    out_dir = os.path.join(dst, rel)
    os.makedirs(out_dir, exist_ok=True)
    for fn in files:
        if fn in IGNORE_FILE_NAMES or fn.endswith(IGNORE_FILE_SUFFIXES):
            continue
        s = os.path.join(root, fn)
        d = os.path.join(out_dir, fn)
        shutil.copy2(s, d)
PY
}

stop_in_dir_then_delete(){
  have astro || die "Astro CLI 'astro' not found."
  if [ -d "$OUT_DIR_ABS" ]; then
    ( cd "$OUT_DIR_ABS"
      if [ "$1" = "stop" ]; then astro dev stop || true; else astro dev kill || true; fi
    )
    rm -rf -- "$OUT_DIR_ABS"
  fi
}

if [ $DO_STOP -eq 1 ]; then stop_in_dir_then_delete "stop"; log "Stopped and removed $OUT_DIR"; exit 0; fi
if [ $DO_KILL -eq 1 ]; then stop_in_dir_then_delete "kill"; log "Killed and removed $OUT_DIR"; exit 0; fi

[ -n "${ENVIRONMENT}" ] || die "--env DEV|QA|UAT|PROD is required"
ENV_LC="$(echo "$ENVIRONMENT" | tr '[:upper:]' '[:lower:]')"
case "$ENV_LC" in dev|qa) GROUP="dna-non-prod" ;; uat|prod) GROUP="dna-prod" ;; *) die "Unknown env: $ENVIRONMENT" ;; esac
[ $DO_RESTART -eq 0 ] || stop_in_dir_then_delete "stop"

ensure_python

CFG_FILE="$ROOT/deployment_configs/${GROUP}/${ENV_LC}/astro_deploy_config.yaml"
DAGS_BASE_ABS="$ROOT/dags"
INCLUDE_BASE_ABS="$ROOT/include"
PLUGINS_BASE_ABS="$ROOT/plugins"
DOCKER_BASE_ABS="$ROOT/dockerfiles"
REQS_BASE_ABS="$ROOT/requirements"
PKGS_BASE_ABS="$ROOT/packages"

[ -f "$CFG_FILE" ]         || die "Config not found: $CFG_FILE"
[ -d "$DAGS_BASE_ABS" ]    || die "Missing dir: $DAGS_BASE_ABS"
[ -d "$INCLUDE_BASE_ABS" ] || die "Missing dir: $INCLUDE_BASE_ABS"
[ -d "$PLUGINS_BASE_ABS" ] || die "Missing dir: $PLUGINS_BASE_ABS"

log "Env: $ENVIRONMENT  (group: $GROUP)"
log "Config: $CFG_FILE"
log "Out: $OUT_DIR_ABS"

rm -rf -- "$OUT_DIR_ABS"
mkdir -p "$OUT_DIR_ABS"/{dags,include,plugins}

copy_list(){
  key="$1"; base_abs="$2"; dest_sub="$3"; expect="$4"

  entries="$(py_list "$key" || true)"
  while IFS= read -r e || [ -n "$e" ]; do
    e="${e%$'\r'}"
    [ -z "$e" ] && continue

    matches="$(py_glob_abs "$base_abs" "$e" || true)"
    [ -n "$matches" ] || { warn "No match for ${base_abs}/${e}"; continue; }

    while IFS= read -r p || [ -n "$p" ]; do
      p="${p%$'\r'}"
      [ -z "$p" ] && continue

      rel="$(py_rel_abs "$base_abs" "$p")"
      dest_abs="$OUT_DIR_ABS/$dest_sub/$rel"

      base_name="$(basename "$p")"
      case "$base_name" in __pycache__|.pytest_cache|.ipynb_checkpoints|.git|.hg|.svn|.idea|.vscode|venv|.venv) continue;; esac
      case "$p" in *.pyc|*.pyo|*.pyd|*/.DS_Store|*/Thumbs.db|.DS_Store|Thumbs.db) continue;; esac

      if [ -d "$p" ] && { [ "$expect" = "dir" ] || [ "$expect" = "any" ]; }; then
        py_copy_dir_filtered "$p" "$dest_abs"
      elif [ -f "$p" ] && { [ "$expect" = "file" ] || [ "$expect" = "any" ]; }; then
        mkdir -p "$(dirname "$dest_abs")"; cp "$p" "$dest_abs"
      fi
    done <<EOF
$matches
EOF
  done <<EOF
$entries
EOF
}

log "Copying DAGs...";     copy_list "dags"    "$DAGS_BASE_ABS"    "dags"    "file"
log "Copying include...";  copy_list "include" "$INCLUDE_BASE_ABS" "include" "dir"
log "Copying plugins...";  copy_list "plugins" "$PLUGINS_BASE_ABS" "plugins" "dir"

DOCKER_NAME="$(py_scalar dockerfile)"
REQS_NAME="$(py_scalar requirements)"
PKGS_NAME="$(py_scalar packages)"
[ -n "$DOCKER_NAME" ] && [ -n "$REQS_NAME" ] && [ -n "$PKGS_NAME" ] || die "Missing 'other_files' entries in $CFG_FILE"

cp "$DOCKER_BASE_ABS/$DOCKER_NAME"   "${OUT_DIR_ABS}/Dockerfile"
cp "$REQS_BASE_ABS/$REQS_NAME"       "${OUT_DIR_ABS}/requirements.txt"
cp "$PKGS_BASE_ABS/$PKGS_NAME"       "${OUT_DIR_ABS}/packages.txt"

mkdir -p "${OUT_DIR_ABS}/.astro"
cp -R "$ROOT/.astro"/. "${OUT_DIR_ABS}/.astro"/ 2>/dev/null || true

log "Build complete. Contents:"
( cd "$ROOT" && find "$OUT_DIR" -maxdepth 4 -print | sort )
[ $DO_START -eq 0 ] && [ $DO_RESTART -eq 0 ] || {
  have astro || die "Astro CLI 'astro' not found. Install it, then re-run."
  log "Starting dev environment…"
  ( cd "$OUT_DIR_ABS" && astro dev start )
}
