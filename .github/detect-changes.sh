#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Decide which Docker images need rebuilding based on the git diff, and assign
# each image a topological level so the workflow can build in the correct order.
#
# Levels are computed from `depends_on_images` graph:
#   level(img) = max(level(dep) for dep in depends_on_images) + 1
#   level(img) = 0              if depends_on_images is empty
#
# Output:
#     { "level_0": {"include":[...]}, "level_1": {"include":[...]}, ... }
# with one entry per level 0..MAX_LEVEL_SUPPORTED.
#
# Requires: yq (mikefarah/yq v4), jq, git, bash >= 4
#
# Environment:
#     CONFIG_PATH   Path to build config.              (default: docker-images/build-config.yml)
#     BASE_SHA      Git SHA to diff from.              (empty => build all)
#     HEAD_SHA      Git SHA to diff to.                (default: HEAD)
#     FORCE_BUILD   "all" or comma-separated "image:version" list.
# ------------------------------------------------------------------------------
set -euo pipefail

CONFIG_PATH="${CONFIG_PATH:-docker-images/build-config.yml}"
BASE_SHA="${BASE_SHA:-}"
HEAD_SHA="${HEAD_SHA:-HEAD}"
FORCE_BUILD="${FORCE_BUILD:-}"
IMAGE_ROOT="docker-images"
ZERO_SHA="0000000000000000000000000000000000000000"
# Must match the number of build-level-N jobs defined in docker-build.yml.
MAX_LEVEL_SUPPORTED=4

log() { printf '%s\n' "$*" >&2; }

# ---- Load config as JSON -----------------------------------------------------
if [[ ! -f "$CONFIG_PATH" ]]; then
  log "ERROR: config file not found: $CONFIG_PATH"
  exit 2
fi
CONFIG_JSON=$(yq -o=json '.' "$CONFIG_PATH")

mapfile -t ALL_IDS < <(jq -r '.images[]? | "\(.image):\(.version)"' <<<"$CONFIG_JSON")

if (( ${#ALL_IDS[@]} == 0 )); then
  log "No images defined in config."
  jq -cn --argjson max "$MAX_LEVEL_SUPPORTED" \
    'reduce range(0; $max + 1) as $i ({}; . + {("level_\($i)"): {include: []}})'
  exit 0
fi

# ---- Validate: unique IDs ----------------------------------------------------
dup=$(printf '%s\n' "${ALL_IDS[@]}" | sort | uniq -d)
if [[ -n "$dup" ]]; then
  log "ERROR: duplicate image ids in config:"
  while IFS= read -r d; do log "  $d"; done <<<"$dup"
  exit 2
fi

# ---- Validate: all depends_on_images references exist ------------------------
ALL_IDS_JSON=$(printf '%s\n' "${ALL_IDS[@]}" | jq -R . | jq -s .)
while IFS= read -r img_json; do
  id=$(jq -r '"\(.image):\(.version)"' <<<"$img_json")
  while IFS= read -r dep; do
    [[ -z "$dep" ]] && continue
    if ! jq -e --arg d "$dep" 'index($d) != null' <<<"$ALL_IDS_JSON" >/dev/null; then
      log "ERROR: image '$id' references unknown depends_on_images entry '$dep'"
      exit 2
    fi
  done < <(jq -r '.depends_on_images[]? // empty' <<<"$img_json")
done < <(jq -c '.images[]' <<<"$CONFIG_JSON")

# ---- Compute topological level for every image ------------------------------
declare -A LEVEL=()

# Level 0: images with no depends_on_images
while IFS= read -r img_json; do
  id=$(jq -r '"\(.image):\(.version)"' <<<"$img_json")
  n=$(jq -r '(.depends_on_images // []) | length' <<<"$img_json")
  [[ "$n" == "0" ]] && LEVEL[$id]=0
done < <(jq -c '.images[]' <<<"$CONFIG_JSON")

# Iteratively raise: level(img) = max(level(dep)) + 1 once all deps resolved
for _ in $(seq 1 50); do
  progressed=0
  while IFS= read -r img_json; do
    id=$(jq -r '"\(.image):\(.version)"' <<<"$img_json")
    [[ -n "${LEVEL[$id]:-}" ]] && continue
    all_known=1
    max_dep=-1
    while IFS= read -r dep_id; do
      [[ -z "$dep_id" ]] && continue
      if [[ -z "${LEVEL[$dep_id]:-}" ]]; then
        all_known=0
        break
      fi
      (( LEVEL[$dep_id] > max_dep )) && max_dep=${LEVEL[$dep_id]}
    done < <(jq -r '.depends_on_images[]? // empty' <<<"$img_json")
    if (( all_known )); then
      LEVEL[$id]=$((max_dep + 1))
      progressed=1
    fi
  done < <(jq -c '.images[]' <<<"$CONFIG_JSON")
  (( progressed == 0 )) && break
done

# Any id without a level means we have a cycle in depends_on_images
for id in "${ALL_IDS[@]}"; do
  if [[ -z "${LEVEL[$id]:-}" ]]; then
    log "ERROR: cycle detected in depends_on_images (could not assign level to '$id')"
    exit 2
  fi
done

# ---- Determine which images to build ----------------------------------------
declare -A SELECTED=()
mark() { SELECTED[$1]=1; }

if [[ -n "$FORCE_BUILD" ]]; then
  if [[ "${FORCE_BUILD,,}" == "all" ]]; then
    log "Forced build: all"
    for id in "${ALL_IDS[@]}"; do mark "$id"; done
  else
    log "Forced build: $FORCE_BUILD"
    IFS=',' read -ra parts <<<"$FORCE_BUILD"
    for raw in "${parts[@]}"; do
      id="${raw// /}"
      [[ -z "$id" ]] && continue
      if ! printf '%s\n' "${ALL_IDS[@]}" | grep -qxF "$id"; then
        log "ERROR: FORCE_BUILD references unknown image: $id"
        exit 2
      fi
      mark "$id"
    done
  fi

elif [[ -z "$BASE_SHA" || "$BASE_SHA" == "$ZERO_SHA" ]]; then
  log "No usable BASE_SHA — treating as initial/forced push; building all."
  for id in "${ALL_IDS[@]}"; do mark "$id"; done

else
  log "Diffing $BASE_SHA...$HEAD_SHA"
  if ! CHANGED=$(git diff --name-only "$BASE_SHA...$HEAD_SHA" 2>/dev/null); then
    log "git diff failed — falling back to building all images."
    for id in "${ALL_IDS[@]}"; do mark "$id"; done
  else
    mapfile -t CHANGED_FILES < <(printf '%s\n' "$CHANGED" | awk 'NF')
    log "Changed files (${#CHANGED_FILES[@]}):"
    for f in "${CHANGED_FILES[@]}"; do log "  $f"; done

    while IFS= read -r img_json; do
      id=$(jq -r '"\(.image):\(.version)"' <<<"$img_json")
      name=$(jq -r '.image'   <<<"$img_json")
      version=$(jq -r '.version' <<<"$img_json")

      watched=("$IMAGE_ROOT/$name/$version")
      while IFS= read -r dep; do
        [[ -n "$dep" ]] && watched+=("${dep%/}")
      done < <(jq -r '.depends_on[]? // empty' <<<"$img_json")

      for f in "${CHANGED_FILES[@]}"; do
        [[ -z "$f" ]] && continue
        for w in "${watched[@]}"; do
          if [[ "$f" == "$w" || "$f" == "$w"/* ]]; then
            log "  $id ← change in '$f' (watch: '$w')"
            mark "$id"
            break 2
          fi
        done
      done
    done < <(jq -c '.images[]' <<<"$CONFIG_JSON")
  fi
fi

# ---- Transitive closure on depends_on_images --------------------------------
changed=1
while (( changed )); do
  changed=0
  while IFS= read -r img_json; do
    id=$(jq -r '"\(.image):\(.version)"' <<<"$img_json")
    [[ -n "${SELECTED[$id]:-}" ]] && continue
    while IFS= read -r dep_id; do
      [[ -z "$dep_id" ]] && continue
      if [[ -n "${SELECTED[$dep_id]:-}" ]]; then
        log "  $id ← transitively via depends_on_images ($dep_id)"
        mark "$id"
        changed=1
        break
      fi
    done < <(jq -r '.depends_on_images[]? // empty' <<<"$img_json")
  done < <(jq -c '.images[]' <<<"$CONFIG_JSON")
done

# ---- Check selected images fit within workflow-supported levels -------------
MAX_LEVEL_SELECTED=0
for id in "${!SELECTED[@]}"; do
  (( LEVEL[$id] > MAX_LEVEL_SELECTED )) && MAX_LEVEL_SELECTED=${LEVEL[$id]}
done
if (( ${#SELECTED[@]} > 0 )) && (( MAX_LEVEL_SELECTED > MAX_LEVEL_SUPPORTED )); then
  log "ERROR: dependency graph is $((MAX_LEVEL_SELECTED + 1)) levels deep;"
  log "       workflow currently supports $((MAX_LEVEL_SUPPORTED + 1)) (level_0 .. level_$MAX_LEVEL_SUPPORTED)."
  log "       Add build-level-$((MAX_LEVEL_SUPPORTED + 1)) in .github/workflows/docker-build.yml"
  log "       and bump MAX_LEVEL_SUPPORTED in this script."
  exit 2
fi

# ---- Build JSON entry list with level assignments ---------------------------
entries_json="[]"
for id in "${!SELECTED[@]}"; do
  img_json=$(jq --arg id "$id" -c '.images[] | select("\(.image):\(.version)" == $id)' <<<"$CONFIG_JSON")
  entry=$(jq -c --argjson lvl "${LEVEL[$id]}" '{
    image:      .image,
    version:    .version,
    context:    (.context    // "."),
    dockerfile: (.dockerfile // "docker-images/\(.image)/\(.version)/Dockerfile"),
    platforms:  (.platforms  // "linux/amd64"),
    _level:     $lvl
  }' <<<"$img_json")
  entries_json=$(jq -c ". + [$entry]" <<<"$entries_json")
done

# ---- Emit plan grouped by level ---------------------------------------------
plan=$(jq -cn --argjson entries "$entries_json" --argjson max "$MAX_LEVEL_SUPPORTED" '
  reduce range(0; $max + 1) as $i ({};
    . + {
      ("level_\($i)"): {
        include: ($entries | map(select(._level == $i)) | map(del(._level)))
      }
    }
  )
')

# ---- Logging ----------------------------------------------------------------
total=${#SELECTED[@]}
log ""
log "Build plan: $total image(s) to build"
for i in $(seq 0 "$MAX_LEVEL_SUPPORTED"); do
  n=$(jq ".level_$i.include | length" <<<"$plan")
  (( n == 0 )) && continue
  log "  level_$i ($n):"
  while IFS= read -r line; do
    log "    - $line"
  done < <(jq -r ".level_$i.include[] | \"\(.image):\(.version)\"" <<<"$plan")
done

echo "$plan"