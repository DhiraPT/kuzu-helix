# Kuzu to HelixDB Migration Tool

A complete migration tool to transfer your existing Kuzu graph database to HelixDB. This guide assumes you're setting up HelixDB for the first time.

## What is HelixDB?

HelixDB is a graph-vector database written in Rust that combines traditional graph capabilities with vector operations for AI/ML workloads. It offers:
- **Graph Database**: Store and query nodes and relationships
- **Vector Search**: Embed and search high-dimensional vectors
- **PyTorch-like Interface**: Familiar query patterns for ML practitioners
- **High Performance**: Rust-based engine for speed and reliability

## Prerequisites

- **Python 3.8+**
- **Existing Kuzu database** you want to migrate
- **10-20 minutes** for setup and migration

## Step 1: Install HelixDB

```bash
# Install the HelixDB Python client
pip install helix-py

# Also install migration dependencies
pip install kuzu pandas pyarrow click tqdm
```

## Step 2: Initialize HelixDB

Initialize a new HelixDB project in your working directory:

```bash
helix init
```

**Important**: This creates a `helix.toml` configuration file in the current directory. The migration tool must run from a directory containing this file (or use `--output` to specify a path with `helix.toml`), as `helix push dev` requires it.

## Step 3: Download the Migration Tool

```bash
# Download this repository or just the migration script
curl -O https://raw.githubusercontent.com/your-repo/kuzu-helix/main/kuzu_to_helix_migrator.py
```

## Step 4: Run the Migration

### Basic Migration (Local HelixDB):

```bash
# Run from the directory where you ran 'helix init'
python kuzu_to_helix_migrator.py --kuzu-path /path/to/your/kuzu.db
```

The migration tool will:
1. Connect to your Kuzu database and discover the schema
2. Generate `schema.hx` and `queries.hx` files in the output directory (default: `./db`)
3. **Pause and prompt you** to deploy the schema
4. Export data to Parquet format
5. Import all nodes and edges to HelixDB

**Note**: By default, generated files go to `./db`. If you want them in the current directory (where `helix.toml` is), use `--output .`

### Advanced Options:

```bash
python kuzu_to_helix_migrator.py \
  --kuzu-path /path/to/your/kuzu.db \
  --output ./migration_output \
  --helix-port 6969 \
  --verbose
```

### Cloud HelixDB:

```bash
python kuzu_to_helix_migrator.py \
  --kuzu-path /path/to/your/kuzu.db \
  --cloud \
  --helix-endpoint https://your-instance.helix.cloud \
  --helix-port 6969
```

### All Options:

- `--kuzu-path, -k`: Path to your Kuzu database (required)
- `--helix-port, -p`: HelixDB port (default: 6969)
- `--helix-endpoint, -e`: API endpoint for cloud instances
- `--local/--cloud`: Use local or cloud HelixDB (default: local)
- `--output, -o`: Directory for migration files (default: ./db)
- `--verbose, -v`: Show detailed progress
- `--skip-deploy`: Skip the deployment prompt

## Step 5: Deploy Schema to HelixDB

When the migration script pauses, open a new terminal and deploy your schema:

```bash
# For development/testing
helix push dev

# For production
helix push prod
```

After deployment completes, return to the migration terminal and press **Enter** to continue.

## Step 6: Verify the Migration

After migration completes, you'll see:

```
✅ Migration completed successfully!
Total nodes migrated: 1,234
  - User: 850
  - City: 234

Total edges migrated: 5,678
  - Follows: 3,456
  - LivesIn: 2,222

Elapsed time: 0:02:15
Output directory: ./db
```

Check the migration report:
```bash
cat db/migration_report.json
```

## Step 7: Query Your Data in HelixDB

Now you can query your migrated data using HelixDB's Python client:

```python
import helix

# Connect to HelixDB
client = helix.Client(local=True, port=6969)

# Use the auto-generated queries from queries.hx
result = client.query("CreateUser", {
    "name": "Alice",
    "age": 30
})
print(result)
```

## What the Migration Does

1. **Schema Discovery**: Automatically detects all node and relationship types in your Kuzu database
2. **Type Mapping**: Maps Kuzu data types to compatible HelixDB types
3. **Data Export**: Efficiently exports data using Parquet format
4. **Schema Creation**: Generates `schema.hx` and `queries.hx` files for HelixDB
5. **Data Import**: Imports all nodes and relationships using HelixDB's query interface
6. **Verification**: Generates detailed migration report with statistics

## Data Type Mapping

| Kuzu Type | HelixDB Type | Notes |
|-----------|--------------|-------|
| INT128, INT64, INT32, INT16, INT8 | I64, I64, I32, I16, I8 | Signed integers (INT128 → I64) |
| SERIAL | I64 | Auto-increment integer |
| UINT64, UINT32, UINT16, UINT8 | U64, U32, U16, U8 | Unsigned integers |
| FLOAT, DOUBLE | F32, F64 | Floating point |
| STRING, BLOB | String | UTF-8 strings / Binary data |
| BOOL | Boolean | Boolean values |
| DATE | Date | **Converted to RFC 3339 with timezone** (e.g., "2024-01-01T00:00:00Z") |
| TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_NS, TIMESTAMP_MS, TIMESTAMP_SEC | Date | **All converted to RFC 3339 with timezone** (e.g., "2024-01-01T12:00:00Z") |
| INTERVAL | String | Time interval as string |
| UUID, INTERNAL_ID | ID | Identifier types |
| LIST, ARRAY | [Type] | Arrays (e.g., [String], [I64]) |
| STRUCT, MAP, UNION | String | Complex types JSON serialized |
| NODE, REL, RECURSIVE_REL | String | Graph types as string |
| ANY, NULL | String | Generic/null types as string |

## Troubleshooting

### "Query CreateXXX failed - check if schema is deployed"
- Ensure you've run `helix push dev` to deploy your schema
- The migration script pauses to prompt you, but if you used `--skip-deploy`, you must deploy manually

### "HelixDB connection failed"
- Ensure HelixDB is initialized with `helix init`
- Check port 6969 is not blocked by firewall
- For cloud instances, verify endpoint and credentials

### "Schema creation errors"
- Check for unsupported property names (avoid special characters)
- Review migration report for detailed error messages
- Some complex nested types may need manual handling

### "Import failures"
- Verify referential integrity in your source data
- Check HelixDB logs for detailed error messages
- Large datasets may require adjusting batch sizes

### "Out of memory"
- Use `--output` to specify a directory with sufficient space
- Process smaller datasets first to test
- Consider cloud instance for large migrations

## Performance Tips

- **Large Databases**: Migration time scales with data size; expect ~1000 nodes/second
- **Complex Properties**: Nested structures are JSON-serialized; consider flattening for better performance
- **Batch Size**: Default batch size (100) works well for most cases
- **Parallel Processing**: For very large datasets, consider splitting by node type

## Next Steps After Migration

1. **Explore HelixDB Features**:
   - Vector similarity search
   - Graph algorithms
   - Text chunking and processing
   - Custom query definitions

2. **Optimize Your Schema**:
   - Add indexes for frequently queried properties
   - Consider denormalization for performance
   - Add vector embeddings for semantic search

3. **Integrate with ML Workflows**:
   - Use embeddings from OpenAI, Gemini, or VoyageAI
   - Process text data with built-in chunking
   - Define custom queries for your specific use cases

## Support

- **HelixDB Documentation**: https://docs.helix-db.com/
- **GitHub Issues**: Report migration issues on this repository
- **Community**: Join HelixDB community for support and discussions

## Example: Complete Migration Workflow

```bash
# 1. Install dependencies
pip install helix-py kuzu pandas pyarrow click tqdm

# 2. Initialize HelixDB
helix init

# 3. Run migration
python kuzu_to_helix_migrator.py --kuzu-path ./my_kuzu_database.db --verbose

# 4. When prompted, deploy schema (in another terminal)
helix push dev

# 5. Press Enter to continue migration

# 6. Verify results
cat db/migration_report.json

# 7. Query your data
python -c "
import helix
client = helix.Client(local=True, port=6969)
result = client.query('CreateUser', {'name': 'Alice', 'age': 30})
print(result)
"
```

Your Kuzu database is now running on HelixDB with full graph and vector capabilities!