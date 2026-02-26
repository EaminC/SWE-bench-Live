#!/usr/bin/env bash
# End-to-end SWE-bench-Live pipeline using Conda
# Usage: ./run_pipeline.sh [issue_pr_map.json]

set -euo pipefail

# ==============================================================================
# 0. CONFIGURATION & PATHS
# ==============================================================================
echo "=============================================="
echo "Initializing SWE-bench-Live Environment..."
echo "=============================================="

SBL_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SWE_FACTORY_ROOT="/home/cc/swe-factory"
STATS_SCRIPT="/home/cc/SWE-bench-live/stats/entry.py"

cd "$SBL_ROOT"

# ==============================================================================
# 1. ENVIRONMENT VARIABLES & KEYS
# ==============================================================================
if [[ -f ".env" ]]; then
    echo "Loading variables from .env..."
    export $(grep -v '^#' .env | xargs)
else
    echo "ERROR: .env file not found in $SBL_ROOT! Please create one." >&2
    exit 1
fi

if [[ -z "${FORGE_API_KEY:-}" ]] || [[ -z "${TAVILY_API_KEY:-}" ]] || [[ -z "${GITHUB_TOKEN:-}" ]]; then
    echo "ERROR: Missing required API keys in .env (FORGE_API_KEY, TAVILY_API_KEY, GITHUB_TOKEN)" >&2
    exit 1
fi

export OPENAI_BASE_URL="${FORGE_BASE_URL:-https://api.forge.tensorblock.co/v1}"
export OPENAI_API_KEY="${FORGE_API_KEY}"
export ANTHROPIC_BASE_URL="https://api.forge.tensorblock.co/v1"
export ANTHROPIC_AUTH_TOKEN="${FORGE_API_KEY}"
export TAVILY_API_KEY="${TAVILY_API_KEY}"

mkdir -p baseline
echo "$GITHUB_TOKEN" > baseline/tokens.txt
echo "✓ Generated baseline/tokens.txt from .env GITHUB_TOKEN"

# ==============================================================================
# 2. CONDA ENVIRONMENTS SETUP
# ==============================================================================
if ! command -v conda &> /dev/null; then
    echo "ERROR: Conda is not installed or not in your PATH." >&2
    echo "Please install Miniconda or Anaconda first." >&2
    exit 1
fi

# 1. Create Main Environment (Python 3.11)
if ! conda info --envs | grep -q "^sbl-main "; then
    echo "Creating main Conda environment (sbl-main) with Python 3.11..."
    conda create -y -n sbl-main python=3.11
    
    echo "Installing dependencies into sbl-main..."
    conda run -n sbl-main pip install --upgrade pip setuptools wheel testresources -q
    conda run -n sbl-main pip install -e . -q
    conda run -n sbl-main pip install openai -q
fi

# 2. Create Launch Environment (Python 3.12)
if ! conda info --envs | grep -q "^sbl-launch "; then
    echo "Creating launch Conda environment (sbl-launch) with Python 3.12..."
    conda create -y -n sbl-launch python=3.12
    
    echo "Installing base dependencies into sbl-launch..."
    conda run -n sbl-launch pip install --upgrade pip setuptools wheel testresources -q
    conda run -n sbl-launch pip install -e . -q
    
    # Install the 'launch' sub-directory or fallback to manual injection
    if [[ -f "launch/pyproject.toml" ]] || [[ -f "launch/setup.py" ]]; then
        echo "Installing RepoLaunch module from ./launch..."
        conda run -n sbl-launch pip install -e ./launch -q
    else
        echo "Warning: package files not found in launch/. Manually injecting known dependencies..."
        conda run -n sbl-launch pip install langchain_community langchain tavily-python langchain-openai -q
    fi
fi

export PYTHONPATH="$SBL_ROOT:${PYTHONPATH:-}"

INPUT_MAP="${1:-baseline/issue_pr_map.json}"
if [[ ! -f "$INPUT_MAP" ]]; then
  echo "File not found: $INPUT_MAP" >&2
  exit 1
fi

count_jsonl() { if [[ -f "$1" ]]; then wc -l < "$1" | tr -d ' '; else echo "0"; fi }
count_json() { if [[ -f "$1" ]]; then conda run -n sbl-main python -c "import json; d=json.load(open('$1')); print(len(d) if isinstance(d, (list, dict)) else 0)" 2>/dev/null || echo "0"; else echo "0"; fi }

# ==============================================================================
# 3. INITIALIZE TRACKING & DIRECTORIES
# ==============================================================================
TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="baseline/runs/${TS}"
LOG_FILE="${RUN_DIR}/sbl_pipeline_log.txt"

mkdir -p "$RUN_DIR"

echo "Input Map: $INPUT_MAP"
echo "All output will be logged to: $LOG_FILE"

# ==============================================================================
# 4. PIPELINE EXECUTION (Wrapped to capture all logs)
# ==============================================================================
{
    echo -e "\n=============================================="
    echo "Starting cost tracker..."
    if [[ -f "$STATS_SCRIPT" ]]; then
        conda run -n sbl-main python "$STATS_SCRIPT" start || echo "Warning: Failed to start stats tracker"
    fi

    echo -e "\n[1/5] Prepare Pull2Issue from Map..."
    conda run -n sbl-main python baseline/sbl_prepare_pull2issue_from_issue_pr_map.py \
      --input "$INPUT_MAP" \
      --cutoff_date 20090101 \
      --gh_token_file baseline/tokens.txt \
      --token_id 0

    echo -e "\n[2/5] Running Curation Pipeline (step2.sh)..."
    chmod +x ./baseline/step2.sh
    # We execute this in the main environment context
    conda run -n sbl-main ./baseline/step2.sh

    echo -e "\n[3/5a] Merge Task Instances & Prepare Launch Config..."
    conda run -n sbl-main python baseline/sbl_step3_prepare_launch_dataset.py

    echo -e "\n[3/5b] Executing RepoLaunch (Generating testable containers)..."
    cd "$SBL_ROOT/launch"
    # Manually create the playground directory to prevent FileNotFoundError
    mkdir -p data/sbl_baseline/playground
    # Execute using the dedicated Python 3.12 environment
    conda run --no-capture-output -n sbl-launch python -m launch.run --config-path data/sbl_baseline/config.json
    cd "$SBL_ROOT"
    
    ORGANIZE_JSONL="launch/data/sbl_baseline/organize.jsonl"
    echo " -> RepoLaunch Generated: $(count_jsonl "$ORGANIZE_JSONL") instances"

    echo -e "\n[4/5] Building F2P Dataset (Validation)..."
    # Manually create the output directory for validation logs
    mkdir -p logs/val
    conda run -n sbl-main python -m evaluation.validation \
      --input_dir "$ORGANIZE_JSONL" \
      --platform linux \
      --workers 4 \
      --output_dir logs/val \
      --overwrite 1
      
    VALIDATED_JSONL="logs/val/validated_instances.jsonl"
    echo " -> Validated Instances: $(count_jsonl "$VALIDATED_JSONL")"

    echo -e "\n[5/5a] Preparing SWE-Factory Judge format..."
    conda run -n sbl-main python baseline/sf_make_judge_f2p_folder_from_organize_jsonl.py \
      --input "$ORGANIZE_JSONL" \
      --out-dir baseline/sf_judge_f2p_outputs \
      --platform linux \
      --workers 2 \
      --overwrite 1

    echo -e "\n[5/5b] Classifying Fail2Pass Status (SWE-Factory)..."
    SUMMARY_JSON="baseline/sf_judge_f2p_summary.json"
    if [[ -f "$SWE_FACTORY_ROOT/scripts/judge_fail2pass.py" ]]; then
        conda run -n sbl-main python "$SWE_FACTORY_ROOT/scripts/judge_fail2pass.py" \
          baseline/sf_judge_f2p_outputs \
          "$SUMMARY_JSON" \
          --processes 20
    else
        echo "Warning: SWE-Factory judge script not found at $SWE_FACTORY_ROOT"
    fi

    # ==============================================================================
    # 5. END TRACKING & METRICS REPORT
    # ==============================================================================
    echo -e "\nWaiting 5 seconds for API metrics to sync..."
    sleep 5
    echo "Ending cost tracker..."
    if [[ -f "$STATS_SCRIPT" ]]; then
        conda run -n sbl-main python "$STATS_SCRIPT" end || echo "Warning: Failed to end stats tracker"
    fi

    echo -e "\n=============================================="
    echo "       SBL PIPELINE FUNNEL REPORT             "
    echo "=============================================="
    echo "1. Initial JSON map        : $(count_json "$INPUT_MAP") items"
    echo "2. RepoLaunch Generated    : $(count_jsonl "$ORGANIZE_JSONL") instances (organize.jsonl)"
    echo "3. Validated F2P/P2P       : $(count_jsonl "$VALIDATED_JSONL") instances"
    echo "4. Final Judged Summary    : $(count_json "$SUMMARY_JSON") items"
    echo "=============================================="
    echo "Master Log File   : $LOG_FILE"
    echo "SWE-Factory Judge : $SBL_ROOT/$SUMMARY_JSON"
    echo "=============================================="

} 2>&1 | tee -a "$LOG_FILE"