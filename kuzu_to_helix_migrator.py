#!/usr/bin/env python3

"""
Kuzu to HelixDB Migration Tool

Migrates graph data from Kuzu to HelixDB using the helix-py client.
"""

import os
import sys
import json
import csv
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import tempfile

import kuzu
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from tqdm import tqdm
import click

try:
    import helix
    from helix.loader import Schema
    from helix import Query, Chunk
    from helix.types import Payload
except ImportError:
    click.echo("Error: helix-py is not installed. Please run: pip install helix-py", err=True)
    sys.exit(1)


class KuzuToHelixMigrator:
    """Handles migration from Kuzu to HelixDB."""
    
    def __init__(self, kuzu_db_path: str, helix_config: Dict[str, Any], output_dir: Optional[str] = None):
        self.kuzu_db_path = kuzu_db_path
        self.helix_config = helix_config
        self.output_dir = Path(output_dir) if output_dir else Path(tempfile.mkdtemp(prefix="kuzu_helix_"))
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.parquet_dir = self.output_dir / "parquet"
        self.parquet_dir.mkdir(exist_ok=True)
        
        # Database connections
        self.kuzu_db = None
        self.kuzu_conn = None
        self.helix_client = None
        self.helix_schema = None
        
        # Migration statistics
        self.stats = {
            "nodes_migrated": {},
            "edges_migrated": {},
            "total_nodes": 0,
            "total_edges": 0,
            "errors": [],
            "start_time": datetime.now()
        }
        
        # Schema mapping
        self.schema_mapping = {}
        self.node_queries = {}
        self.edge_queries = {}
    
    def connect_kuzu(self):
        """Connect to Kuzu database."""
        try:
            self.kuzu_db = kuzu.Database(self.kuzu_db_path)
            self.kuzu_conn = kuzu.Connection(self.kuzu_db)
            click.echo(f"✓ Connected to Kuzu database: {self.kuzu_db_path}")
        except Exception as e:
            click.echo(f"✗ Failed to connect to Kuzu: {e}", err=True)
            raise
    
    def connect_helix(self):
        """Connect to HelixDB instance."""
        try:
            if self.helix_config.get('local', True):
                self.helix_client = helix.Client(
                    local=True,
                    port=self.helix_config.get('port', 6969),
                    verbose=self.helix_config.get('verbose', False)
                )
            else:
                self.helix_client = helix.Client(
                    api_endpoint=self.helix_config['api_endpoint'],
                    port=self.helix_config.get('port', 6969),
                    verbose=self.helix_config.get('verbose', False)
                )
            
            click.echo(f"✓ Connected to HelixDB (port: {self.helix_config.get('port', 6969)})")
            
            # Initialize schema
            self.helix_schema = Schema()
            
        except Exception as e:
            click.echo(f"✗ Failed to connect to HelixDB: {e}", err=True)
            raise
    
    def discover_kuzu_schema(self) -> Dict[str, Any]:
        """Discover schema from Kuzu database."""
        click.echo("\n📊 Discovering Kuzu schema...")
        
        schema = {
            "nodes": {},
            "edges": {}
        }
        
        try:
            # Get all tables
            all_tables = self.kuzu_conn.execute("CALL show_tables() RETURN *;").get_as_df()
            
            if not all_tables.empty:
                # Filter node tables
                node_tables = all_tables[all_tables['type'] == 'NODE']
                for _, row in node_tables.iterrows():
                    table_name = row['name']
                    properties = self._get_table_properties(table_name)
                    schema["nodes"][table_name] = properties
                    click.echo(f"  Found node table: {table_name} ({len(properties)} properties)")
                
                # Filter relationship tables
                rel_tables = all_tables[all_tables['type'] == 'REL']
                for _, row in rel_tables.iterrows():
                    table_name = row['name']
                    properties = self._get_table_properties(table_name)
                    schema["edges"][table_name] = properties
                    click.echo(f"  Found edge table: {table_name} ({len(properties)} properties)")
            
            return schema
            
        except Exception as e:
            click.echo(f"✗ Schema discovery failed: {e}", err=True)
            raise
    
    def _get_table_properties(self, table_name: str) -> Dict[str, str]:
        """Get properties for a Kuzu table."""
        properties = {}
        
        try:
            result = self.kuzu_conn.execute(f"CALL table_info('{table_name}') RETURN *;").get_as_df()
            
            for _, prop in result.iterrows():
                prop_name = prop['property']
                prop_type = prop['type']
                
                # Map Kuzu types to HelixDB types
                helix_type = self._map_type_to_helix(prop_type)
                properties[prop_name] = helix_type
                
        except Exception as e:
            click.echo(f"  Warning: Could not get properties for {table_name}: {e}")
        
        return properties
    
    def _map_type_to_helix(self, kuzu_type: str) -> str:
        """Map Kuzu data types to HelixDB types."""
        type_mapping = {
            'INT64': 'I64',
            'INT32': 'I32',
            'INT16': 'I16',
            'INT8': 'I8',
            'UINT64': 'U64',
            'UINT32': 'U32',
            'UINT16': 'U16',
            'UINT8': 'U8',
            'FLOAT': 'F32',
            'DOUBLE': 'F64',
            'STRING': 'String',
            'BOOL': 'Bool',
            'DATE': 'String',  # Store dates as strings
            'TIMESTAMP': 'String',  # Store timestamps as strings
            'LIST': 'String',  # Serialize lists as JSON strings
            'STRUCT': 'String',  # Serialize structs as JSON strings
            'MAP': 'String'  # Serialize maps as JSON strings
        }
        
        # Handle parameterized types
        base_type = kuzu_type.split('(')[0].split('<')[0].upper()
        return type_mapping.get(base_type, 'String')
    
    def create_helix_schema(self, kuzu_schema: Dict[str, Any]):
        """Create corresponding schema in HelixDB."""
        click.echo("\n🔨 Creating HelixDB schema...")
        
        # Create node types
        for node_name, properties in kuzu_schema["nodes"].items():
            click.echo(f"  Creating node type: {node_name}")
            self.helix_schema.create_node(node_name, properties)
            
            # Create a custom Query class for this node type
            self._create_node_query_class(node_name, properties)
        
        # Create edge types
        for edge_name, properties in kuzu_schema["edges"].items():
            click.echo(f"  Creating edge type: {edge_name}")
            
            # For edges, we need to determine source and target node types
            # This is a simplified approach - in production you'd query Kuzu for the actual constraints
            # For now, we'll make edges generic between any nodes
            self.helix_schema.create_edge(edge_name, "Node", "Node")
            
            # Create a custom Query class for this edge type
            self._create_edge_query_class(edge_name, properties)
        
        # Save schema
        self.helix_schema.save()
        click.echo("✓ HelixDB schema created")
    
    def _create_node_query_class(self, node_type: str, properties: Dict[str, str]):
        """Dynamically create a Query class for inserting nodes."""
        
        class DynamicNodeQuery(Query):
            def __init__(self, **kwargs):
                super().__init__()
                self.data = kwargs
                self.node_type = node_type
            
            def query(self) -> Payload:
                return [{
                    "_type": self.node_type,
                    **self.data
                }]
            
            def response(self, response):
                return response
        
        self.node_queries[node_type] = DynamicNodeQuery
    
    def _create_edge_query_class(self, edge_type: str, properties: Dict[str, str]):
        """Dynamically create a Query class for inserting edges."""
        
        class DynamicEdgeQuery(Query):
            def __init__(self, source_id: Any, target_id: Any, **kwargs):
                super().__init__()
                self.source_id = source_id
                self.target_id = target_id
                self.data = kwargs
                self.edge_type = edge_type
            
            def query(self) -> Payload:
                return [{
                    "_type": self.edge_type,
                    "_source": self.source_id,
                    "_target": self.target_id,
                    **self.data
                }]
            
            def response(self, response):
                return response
        
        self.edge_queries[edge_type] = DynamicEdgeQuery
    
    def export_to_parquet(self, kuzu_schema: Dict[str, Any]):
        """Export Kuzu data to Parquet files for efficient loading."""
        click.echo("\n📦 Exporting data to Parquet format...")
        
        # Export nodes
        for node_table in kuzu_schema["nodes"]:
            self._export_node_table_to_parquet(node_table)
        
        # Export edges
        for edge_table in kuzu_schema["edges"]:
            self._export_edge_table_to_parquet(edge_table)
    
    def _export_node_table_to_parquet(self, table_name: str):
        """Export a node table to Parquet."""
        try:
            query = f"MATCH (n:{table_name}) RETURN n"
            result = self.kuzu_conn.execute(query).get_as_df()
            
            if result.empty:
                click.echo(f"  No data in node table: {table_name}")
                return
            
            # Process the data
            processed_data = []
            for _, row in result.iterrows():
                node_data = row['n']
                if isinstance(node_data, dict):
                    # Add type information
                    node_data['_node_type'] = table_name
                    processed_data.append(node_data)
            
            if processed_data:
                df = pd.DataFrame(processed_data)
                
                # Convert complex types to JSON strings
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                
                # Save to Parquet
                output_file = self.parquet_dir / f"{table_name}_nodes.parquet"
                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_file)
                
                click.echo(f"  ✓ Exported {len(df)} nodes from {table_name} to {output_file.name}")
                self.stats["nodes_migrated"][table_name] = len(df)
                self.stats["total_nodes"] += len(df)
            
        except Exception as e:
            error_msg = f"Failed to export node table {table_name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)
    
    def _export_edge_table_to_parquet(self, table_name: str):
        """Export an edge table to Parquet."""
        try:
            query = f"MATCH (src)-[r:{table_name}]->(dst) RETURN src, r, dst"
            result = self.kuzu_conn.execute(query).get_as_df()
            
            if result.empty:
                click.echo(f"  No data in edge table: {table_name}")
                return
            
            # Process the data
            processed_data = []
            for _, row in result.iterrows():
                edge_data = {}
                
                # Extract source and target IDs
                src = row['src']
                dst = row['dst']
                rel = row['r']
                
                if isinstance(src, dict):
                    edge_data['_source_id'] = self._extract_id(src)
                    edge_data['_source_type'] = src.get('_label', 'Unknown')
                
                if isinstance(dst, dict):
                    edge_data['_target_id'] = self._extract_id(dst)
                    edge_data['_target_type'] = dst.get('_label', 'Unknown')
                
                # Add edge properties
                if isinstance(rel, dict):
                    for key, value in rel.items():
                        if key not in ['_src', '_dst']:
                            edge_data[key] = value
                
                edge_data['_edge_type'] = table_name
                processed_data.append(edge_data)
            
            if processed_data:
                df = pd.DataFrame(processed_data)
                
                # Convert complex types to JSON strings
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (list, dict)) else x)
                
                # Save to Parquet
                output_file = self.parquet_dir / f"{table_name}_edges.parquet"
                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_file)
                
                click.echo(f"  ✓ Exported {len(df)} edges from {table_name} to {output_file.name}")
                self.stats["edges_migrated"][table_name] = len(df)
                self.stats["total_edges"] += len(df)
            
        except Exception as e:
            error_msg = f"Failed to export edge table {table_name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)
    
    def _extract_id(self, node: Dict) -> Any:
        """Extract ID from a node."""
        if '_id' in node:
            return node['_id']
        elif 'id' in node:
            return node['id']
        else:
            # Look for any field containing 'id'
            for key in node.keys():
                if 'id' in key.lower():
                    return node[key]
        
        # Fallback to hash
        return str(hash(json.dumps(node, sort_keys=True, default=str)))
    
    def import_to_helix(self):
        """Import Parquet files to HelixDB."""
        click.echo("\n📥 Importing data to HelixDB...")
        
        # Import nodes first
        node_files = list(self.parquet_dir.glob("*_nodes.parquet"))
        for node_file in tqdm(node_files, desc="Importing nodes"):
            self._import_node_file(node_file)
        
        # Then import edges
        edge_files = list(self.parquet_dir.glob("*_edges.parquet"))
        for edge_file in tqdm(edge_files, desc="Importing edges"):
            self._import_edge_file(edge_file)
    
    def _import_node_file(self, parquet_file: Path):
        """Import nodes from a Parquet file."""
        try:
            # Read Parquet file
            table = pq.read_table(parquet_file)
            df = table.to_pandas()
            
            if df.empty:
                return
            
            node_type = parquet_file.stem.replace('_nodes', '')
            query_class = self.node_queries.get(node_type)
            
            if not query_class:
                click.echo(f"  Warning: No query class for node type {node_type}")
                return
            
            # Batch import
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Convert row to dict and clean up
                    data = row.to_dict()
                    data.pop('_node_type', None)
                    
                    # Deserialize JSON strings back to objects
                    for key, value in data.items():
                        if isinstance(value, str) and value.startswith(('[', '{')):
                            try:
                                data[key] = json.loads(value)
                            except:
                                pass
                    
                    # Create and execute query
                    query = query_class(**data)
                    self.helix_client.query(query)
            
        except Exception as e:
            error_msg = f"Failed to import {parquet_file.name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)
    
    def _import_edge_file(self, parquet_file: Path):
        """Import edges from a Parquet file."""
        try:
            # Read Parquet file
            table = pq.read_table(parquet_file)
            df = table.to_pandas()
            
            if df.empty:
                return
            
            edge_type = parquet_file.stem.replace('_edges', '')
            query_class = self.edge_queries.get(edge_type)
            
            if not query_class:
                click.echo(f"  Warning: No query class for edge type {edge_type}")
                return
            
            # Batch import
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Extract source and target
                    source_id = row['_source_id']
                    target_id = row['_target_id']
                    
                    # Prepare edge properties
                    data = row.to_dict()
                    for key in ['_source_id', '_target_id', '_source_type', '_target_type', '_edge_type']:
                        data.pop(key, None)
                    
                    # Deserialize JSON strings
                    for key, value in data.items():
                        if isinstance(value, str) and value.startswith(('[', '{')):
                            try:
                                data[key] = json.loads(value)
                            except:
                                pass
                    
                    # Create and execute query
                    query = query_class(source_id, target_id, **data)
                    self.helix_client.query(query)
            
        except Exception as e:
            error_msg = f"Failed to import {parquet_file.name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)
    
    def generate_report(self):
        """Generate migration report."""
        click.echo("\n📊 Migration Report")
        click.echo("=" * 60)
        
        elapsed = datetime.now() - self.stats["start_time"]
        
        click.echo(f"Total nodes migrated: {self.stats['total_nodes']:,}")
        for node_type, count in self.stats["nodes_migrated"].items():
            click.echo(f"  - {node_type}: {count:,}")
        
        click.echo(f"\nTotal edges migrated: {self.stats['total_edges']:,}")
        for edge_type, count in self.stats["edges_migrated"].items():
            click.echo(f"  - {edge_type}: {count:,}")
        
        if self.stats["errors"]:
            click.echo(f"\n⚠️  Errors encountered: {len(self.stats['errors'])}")
            for error in self.stats["errors"][:5]:
                click.echo(f"  - {error}")
        
        click.echo(f"\nElapsed time: {elapsed}")
        click.echo(f"Output directory: {self.output_dir}")
        
        # Save report to file
        report_file = self.output_dir / "migration_report.json"
        with open(report_file, 'w') as f:
            json.dump(self.stats, f, indent=2, default=str)
        click.echo(f"Report saved to: {report_file}")
    
    def run(self):
        """Execute the complete migration."""
        click.echo("\n" + "="*60)
        click.echo("Kuzu to HelixDB Migration")
        click.echo("="*60)
        
        try:
            # Connect to databases
            self.connect_kuzu()
            self.connect_helix()
            
            # Discover and create schema
            kuzu_schema = self.discover_kuzu_schema()
            self.create_helix_schema(kuzu_schema)
            
            # Export and import data
            self.export_to_parquet(kuzu_schema)
            self.import_to_helix()
            
            # Generate report
            self.generate_report()
            
            click.echo("\n✅ Migration completed successfully!")
            
        except Exception as e:
            click.echo(f"\n❌ Migration failed: {e}", err=True)
            raise
        
        finally:
            # Clean up connections
            if self.kuzu_conn:
                self.kuzu_conn.close()


@click.command()
@click.option('--kuzu-path', '-k', required=True, help='Path to Kuzu database')
@click.option('--helix-port', '-p', default=6969, help='HelixDB port (default: 6969)')
@click.option('--helix-endpoint', '-e', help='HelixDB API endpoint (for cloud instances)')
@click.option('--local/--cloud', default=True, help='Use local HelixDB instance (default: local)')
@click.option('--output', '-o', help='Output directory for intermediate files')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
def main(kuzu_path, helix_port, helix_endpoint, local, output, verbose):
    """
    Migrate graph data from Kuzu to HelixDB.
    
    This tool exports data from Kuzu to Parquet format and imports it into HelixDB
    using the helix-py client library.
    
    Example:
        python kuzu_to_helix_migrator.py -k /path/to/kuzu.db -p 6969
    """
    
    if not os.path.exists(kuzu_path):
        click.echo(f"Error: Kuzu database not found at {kuzu_path}", err=True)
        sys.exit(1)
    
    # Configure HelixDB connection
    helix_config = {
        'local': local,
        'port': helix_port,
        'verbose': verbose
    }
    
    if not local and helix_endpoint:
        helix_config['api_endpoint'] = helix_endpoint
    elif not local:
        click.echo("Error: --helix-endpoint required for cloud instances", err=True)
        sys.exit(1)
    
    # Run migration
    migrator = KuzuToHelixMigrator(kuzu_path, helix_config, output)
    migrator.run()


if __name__ == "__main__":
    main()