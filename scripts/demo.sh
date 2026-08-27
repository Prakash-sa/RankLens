#!/usr/bin/env bash
set -euo pipefail

project_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
build_dir="${project_root}/build"
results_dir="${project_root}/ranklens-results"

cmake -S "${project_root}" -B "${build_dir}" -DCMAKE_BUILD_TYPE=Release
cmake --build "${build_dir}" --parallel

library_suffix="so"
preload_variable="LD_PRELOAD"
if [[ "$(uname -s)" == "Darwin" ]]; then
  library_suffix="dylib"
  preload_variable="DYLD_INSERT_LIBRARIES"
  export DYLD_FORCE_FLAT_NAMESPACE=1
fi

library="${build_dir}/src/interceptor/libranklens_mpi.${library_suffix}"
executable="${build_dir}/examples/ranklens_ping_pong"
rm -rf "${results_dir}"
mkdir -p "${results_dir}"

export RANKLENS_OUTPUT_DIR="${results_dir}"
export "${preload_variable}=${library}"
mpirun -n 2 "${executable}"
PYTHONPATH="${project_root}/python" python3 -m ranklens.cli analyze "${results_dir}" \
  --html "${results_dir}/report.html" \
  --json "${results_dir}/analysis.json"

