#!/bin/sh

set -eu

: "${WAGS_TAILS_DIR:?set WAGS_TAILS_DIR to your source data directory}"
if [ ! -d "$WAGS_TAILS_DIR" ] || [ ! -w "$WAGS_TAILS_DIR" ]; then
  echo "WAGS_TAILS_DIR must be an existing directory writable by the container user: $WAGS_TAILS_DIR" >&2
  exit 1
fi

HAS_DRUGBANK_DATA=false
for drugbank_file in "$WAGS_TAILS_DIR"/drugbank/drugbank_*.csv; do
  if [ -f "$drugbank_file" ] && [ -r "$drugbank_file" ] && [ -s "$drugbank_file" ]; then
    HAS_DRUGBANK_DATA=true
    break
  fi
done

case "${1:-}" in
drugbank)
  if [ "$HAS_DRUGBANK_DATA" = false ]; then
    echo "DrugBank update requires a readable, nonempty drugbank_<version>.csv in $WAGS_TAILS_DIR/drugbank." >&2
    exit 1
  fi

  drugbank_file=$(wags-tails get-latest drugbank --from_local --silent)
  echo "Using DrugBank file: $drugbank_file"
  exec thera-py update drugbank --use_existing
  ;;
"") ;;
*)
  echo "Usage: $0 [drugbank]" >&2
  exit 1
  ;;
esac

if thera-py check-db; then
  echo "Data already exists. Skipping ETL."
else
  echo "Database status check failed. Running ETL..."
  if [ "$HAS_DRUGBANK_DATA" = true ]; then
    drugbank_file=$(wags-tails get-latest drugbank --from_local --silent)
    echo "Using DrugBank file: $drugbank_file"
    thera-py update drugbank --use_existing
  else
    echo "Skipping DrugBank: no local drugbank_<version>.csv provided."
  fi

  for source in wikidata ncit chemidplus rxnorm hemonc drugsatfda guidetopharmacology chembl; do
    thera-py update "$source"
  done

  thera-py update --normalize
  echo "ETL completed."
fi

exec uvicorn therapy.main:app --port 80 --host 0.0.0.0
