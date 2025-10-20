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

## Step 2: Set Up HelixDB Instance

HelixDB can run locally or in the cloud. For first-time setup, we'll use a local instance:

### Option A: Local Instance (Recommended for Testing)

```python
# save this as setup_helix.py
from helix.instance import Instance
import time

def setup_helix():
    print("🚀 Setting up local HelixDB instance...")
    
    # Create instance (stores config in 'helixdb-cfg' directory)
    helix_instance = Instance("helixdb-cfg", 6969, verbose=True)
    
    # Deploy and start
    helix_instance.deploy()
    helix_instance.start()
    
    print("✅ HelixDB running on localhost:6969")
    print("   Press Ctrl+C to stop")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\\n🛑 Stopping HelixDB...")
        helix_instance.stop()

if __name__ == "__main__":
    setup_helix()
```

Run it:
```bash
python setup_helix.py
```

Keep this terminal open - HelixDB needs to stay running during migration.

### Option B: Cloud Instance

If you have a HelixDB cloud instance, note your endpoint URL and credentials for Step 4.

## Step 3: Download the Migration Tool

```bash
# Download this repository or just the migration script
curl -O https://raw.githubusercontent.com/your-repo/kuzu-helix/main/kuzu_to_helix_migrator.py
```

## Step 4: Run the Migration

### Basic Migration (Local HelixDB):

```bash
python kuzu_to_helix_migrator.py --kuzu-path /path/to/your/kuzu.db
```

### Advanced Options:

```bash
python kuzu_to_helix_migrator.py \\
  --kuzu-path /path/to/your/kuzu.db \\
  --output ./migration_output \\
  --helix-port 6969 \\
  --verbose
```

### Cloud HelixDB:

```bash
python kuzu_to_helix_migrator.py \\
  --kuzu-path /path/to/your/kuzu.db \\
  --cloud \\
  --helix-endpoint https://your-instance.helix.cloud \\
  --helix-port 6969
```

### All Options:

- `--kuzu-path, -k`: Path to your Kuzu database (required)
- `--helix-port, -p`: HelixDB port (default: 6969)
- `--helix-endpoint, -e`: API endpoint for cloud instances
- `--local/--cloud`: Use local or cloud HelixDB (default: local)
- `--output, -o`: Directory for migration files (default: auto-generated)
- `--verbose, -v`: Show detailed progress

## Step 5: Verify the Migration

After migration completes, you'll see:

```
✅ Migration completed successfully!
  Total nodes migrated: 1,234
  Total edges migrated: 5,678
  Output directory: ./migration_output_TIMESTAMP
```

Check the migration report:
```bash
cat migration_output_*/migration_report.json
```

## Step 6: Query Your Data in HelixDB

Now you can query your migrated data using HelixDB's Python client:

```python
import helix

# Connect to HelixDB
db = helix.Client(local=True, port=6969)

# Define a custom query class
from helix import Query
from helix.types import Payload

class find_nodes(Query):
    def __init__(self, node_type: str, limit: int = 10):
        super().__init__()
        self.node_type = node_type
        self.limit = limit
    
    def query(self) -> Payload:
        return [{
            "operation": "find_nodes",
            "type": self.node_type,
            "limit": self.limit
        }]

# Query your data
result = db.query(find_nodes("Person", 5))
print(result)
```

## What the Migration Does

1. **Schema Discovery**: Automatically detects all node and relationship types in your Kuzu database
2. **Type Mapping**: Maps Kuzu data types to compatible HelixDB types
3. **Data Export**: Efficiently exports data using Parquet format
4. **Schema Creation**: Creates corresponding node and edge types in HelixDB
5. **Data Import**: Imports all nodes and relationships using HelixDB's Query interface
6. **Verification**: Generates detailed migration report with statistics

## Data Type Mapping

| Kuzu Type | HelixDB Type | Notes |
|-----------|--------------|-------|
| INT64/INT32 | I64/I32 | Integer types |
| UINT64/UINT32 | U64/U32 | Unsigned integers |
| FLOAT/DOUBLE | F32/F64 | Floating point |
| STRING | String | UTF-8 strings |
| BOOL | Bool | Boolean values |
| DATE/TIMESTAMP | String | Converted to ISO format |
| LIST/STRUCT/MAP | String | JSON serialized |

## Adding Vector Capabilities

After migration, you can enhance your graph with vector embeddings:

```python
from helix.embedding.openai_client import OpenAIEmbedder

# Add embeddings to your nodes
embedder = OpenAIEmbedder()  # Requires OPENAI_API_KEY

# Example: Add embeddings to person names
persons = db.query(find_nodes("Person", 100))
for person in persons:
    # Create embedding from person's name or bio
    embedding = embedder.embed(person['name'])
    # Store embedding back to HelixDB
    # ... (implementation depends on your schema)
```

## Troubleshooting

### "HelixDB connection failed"
- Ensure HelixDB instance is running (`python setup_helix.py`)
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

# 2. Start HelixDB (in one terminal)
python setup_helix.py

# 3. Run migration (in another terminal)
python kuzu_to_helix_migrator.py --kuzu-path ./my_kuzu_database.db --verbose

# 4. Verify results
cat migration_output_*/migration_report.json

# 5. Query your data
python -c "
import helix
db = helix.Client(local=True, port=6969)
# Your queries here...
"
```

Your Kuzu database is now running on HelixDB with full graph and vector capabilities!