#!/bin/sh
set -e


case "$MODE" in


  test)
   
    echo "Adding to database..."
    
  ;;
  incremental_pipeline)
    echo "Running incremental pipeline..."
    echo "Initializing state manager database..."
    #sqlite3 /app/data.db < /app/app/sql/create.sql
    python -m app.incremental_pipeline
    echo "incremental pipeline done"
    cp /app/app/dbt/logs/* /app/app/csv/
    echo "logs available in csv folder"
    
    ;;

  incremental_pipeline_backfill)
   
    echo "Running incremental pipeline backfill"
    python -m app.incremental_pipeline --mode backfill
    echo "incremental pipeline done in backfill mode"
    cp /app/app/dbt/logs/* /app/app/csv/
    echo "logs available in csv folder"
  ;;

  *)
    echo "Unknown MODE: $MODE"
    exit 1
    ;;
    esac