# BigQuery Audit Report

**Project:** of-scheduler-proj  
**Generated:** Tue Sep  9 16:56:52 MDT 2025  
**Audit Type:** Organized Multi-File Output

## Summary
- **Datasets:** 8
- **Tables:** 44
- **Views:** 87
- **Materialized Views:** 0
- **Routines:** 31
- **Scheduled Queries:** 12

## File Structure

```
bigquery_audit_20250909_165244/
├── project/
│   ├── metadata.json          # Project info and audit metadata
│   ├── scheduled_queries.json # All scheduled queries
│   └── summary.json          # Overall counts and statistics
├── datasets/
│   ├── [dataset_name]/
│   │   ├── info.json         # Dataset metadata and access control
│   │   ├── tables.json       # All tables with schema and stats
│   │   ├── views.json        # All views with definitions
│   │   ├── materialized_views.json # Materialized views
│   │   ├── routines.json     # Functions and procedures
│   │   └── summary.json      # Dataset-level counts
└── README.md                 # This file
```

## Usage Examples

### List all datasets
```bash
ls datasets/
```

### Find all tables across datasets
```bash
find datasets -name "tables.json" -exec jq -r '.[].id' {} +
```

### Get total row count for all tables
```bash
find datasets -name "tables.json" -exec jq -r '.[].num_rows' {} + | paste -sd+ - | bc
```

### Find views that reference a specific table
```bash
find datasets -name "views.json" -exec grep -l "your_table_name" {} +
```

## Benefits of This Structure
- **Manageable file sizes**: No single large file to overwhelm editors
- **Easy navigation**: Find specific datasets and objects quickly
- **Selective processing**: Work with individual datasets independently
- **Scalable**: Handles projects with hundreds of datasets efficiently
- **Tool-friendly**: Each JSON file can be processed by standard tools

