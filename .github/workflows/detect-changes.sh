#!/usr/bin/env bash
# ------------------------------------------------------------------------------
# Decide which Docker images need rebuilding based on the git diff.
#
# Reads build-config.yml, diffs BASE_SHA..HEAD_SHA, and prints a JSON plan:
#
#     {
#       "base":    { "include": [ ... images with no depends_on_images ... ] },
#       "derived": { "include": [ ... images that depend on other images ... ] }
#     }
#
# The two-level split (base / derived) lets the workflow run base builds first
# and derived builds after, so a `FROM base-image` reference in a derived image
# resolves to the just-pushed version.
#
# Requires: yq (mikefarah/yq v4), jq, git, bash >= 4
#
# Environment:
#     CONFIG_PATH   Path to build config.              (default: build-config.yml)
#     BASE_SHA      Git SHA to diff from.              (empty => build all)
#     HEAD_SHA      Git SHA to diff to.                (default: HEAD)
#     FORCE_BUILD   "all" or comma-separated "image:version" list.
#
# Writes the JSON plan to stdout. All logging goes to stderr.
# ------------------------------------------------------------------------------
set -euo pipefail

CONFIG_PATH="${CONFIG_PATH:-docker-images/build-config.yml}"
BASE_SHA="${BASE_SHA:-}"
HEAD_SHA="${HEAD_SHA:-HEAD}"
FORCE_BUILD="${FORCE_BUILD:-}"
IMAGE_ROOT="docker-images"
ZERO_SHA="0000000000000000000000000000000000000000"

log() { printf '%s\n' "$*" >&2; }

# ---- Load config as JSON (once) ----------------------------------------------
if [[ ! -f "$CONFIG_PATH" ]]; then
  log "ERROR: config file not found: $CONFIG_PATH"
  exit 2
fi
CONFIG_JSON=$(yq -o=json '.' "$CONFIG_PATH")

# ---- Validate: unique image IDs ---------------------------------------------
mapfile -t ALL_IDS < <(jq -r '.images[]? | "\(.image):\(.version)"' <<<"$CONFIG_JSON")

if (( ${#ALL_IDS[@]} == 0 )); then
  log "No images defined in config."
  jq -cn '{base:{include:[]}, derived:{include:[]}}'
  exit 0
fi

dup=$(printf '%s\n' "${ALL_IDS[@]}" | sort | uniq -d)
if [[ -n "$dup" ]]; then
  log "ERROR: duplicate image ids in config:"
  while IFS= read -r d; do log "  $d"; done <<<"$dup"
  exit 2
fi

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

    # Match each image's watched paths against the changed file list
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

# ---- Transitive closure: if B `depends_on_images` A and A is selected, add B.
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

# ---- Emit plan split into base / derived -------------------------------------
if (( ${#SELECTED[@]} == 0 )); then
  SELECTED_JSON='[]'
else
  SELECTED_JSON=$(printf '%s\n' "${!SELECTED[@]}" | jq -R . | jq -s .)
fi

plan=$(jq -c --argjson sel "$SELECTED_JSON" '
  .images
  | map(select(("\(.image):\(.version)") as $id | $sel | index($id)))
  | map({
      image:      .image,
      version:    .version,
      context:    (.context    // "."),
      dockerfile: (.dockerfile // "docker-images/\(.image)/\(.version)/Dockerfile"),
      platforms:  (.platforms  // "linux/amd64"),
      _derived:   ((.depends_on_images // []) | length > 0)
    })
  | {
      base:    { include: (map(select(._derived | not)) | map(del(._derived))) },
      derived: { include: (map(select(._derived))       | map(del(._derived))) }
    }
' <<<"$CONFIG_JSON")

base_n=$(jq '.base.include    | length' <<<"$plan")
deriv_n=$(jq '.derived.include | length' <<<"$plan")
log ""
log "Plan: $((base_n + deriv_n)) image(s) — $base_n base, $deriv_n derived"
jq -r '.base.include[]    | "  [base]    \(.image):\(.version)"' <<<"$plan" >&2 || true
jq -r '.derived.include[] | "  [derived] \(.image):\(.version)"' <<<"$plan" >&2 || true

echo "$plan"
