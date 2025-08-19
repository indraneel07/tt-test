#!/usr/bin/env bash
set -e

# usage:
#   export ENV=dev   # or qa | uat | prod
#   ./stage_local.sh

ENV=${ENV:?ENV not set (e.g., export ENV=dev)}

# map env -> group
if [[ "$ENV" == "dev" || "$ENV" == "qa" ]]; then
  ENV_GROUP="non-prod"
else
  ENV_GROUP="prod"
fi

rm -rf astro_project && mkdir -p astro_project

# ---- dags (yaml lists file paths) ----
DAGS_MANIFEST="dags/dags_environment/${ENV_GROUP}/dags_${ENV}.yaml"
mkdir -p astro_project/dags
yq e '.[]' "$DAGS_MANIFEST" | while read -r f; do
  cp "dags/$f" astro_project/dags/
done

# ---- include (yaml lists subfolders under include/) ----
INCLUDE_MANIFEST="include/include_environment/${ENV_GROUP}/include_${ENV}.yaml"
mkdir -p astro_project/include
yq e '.[]' "$INCLUDE_MANIFEST" | while read -r pkg; do
  cp -r "include/$pkg" astro_project/include/
done

# ---- plugins (yaml lists subfolders under plugins/) ----
PLUGINS_MANIFEST="plugins/plugins_environment/${ENV_GROUP}/plugins_${ENV}.yaml"
mkdir -p astro_project/plugins
yq e '.[]' "$PLUGINS_MANIFEST" | while read -r plg; do
  cp -r "plugins/$plg" astro_project/plugins/
done

# ---- dockerfile / packages / requirements ----
cp "dockerfiles/${ENV_GROUP}/Dockerfile_${ENV}"        astro_project/Dockerfile
cp "packages/${ENV_GROUP}/packages_${ENV}.txt"         astro_project/packages.txt
cp "requirements/${ENV_GROUP}/requirements_${ENV}.txt" astro_project/requirements.txt

# ---- .astro (copy if exists, else create) ----
if [ -d ".astro" ]; then
  cp -r .astro astro_project/.astro
else
  mkdir -p astro_project/.astro
fi

echo "astro_project built for ENV=$ENV (group=$ENV_GROUP)"
