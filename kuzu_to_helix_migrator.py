#!/usr/bin/env python3

"""
Kuzu to HelixDB Migration Tool

Migrates graph data from Kuzu to HelixDB using the helix-py client.
"""

import os
import sys
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, date, timezone

import kuzu
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import click

try:
    import helix
    from helix.schema import Schema
except ImportError:
    click.echo("Error: helix-py is not installed. Please run: pip install helix-py", err=True)
    sys.exit(1)


class KuzuToHelixMigrator:
    """Handles migration from Kuzu to HelixDB."""

    def __init__(self, kuzu_db_path: str, helix_config: Dict[str, Any], output_dir: Optional[str] = None):
        self.kuzu_db_path = kuzu_db_path
        self.helix_config = helix_config
        self.output_dir = Path(output_dir) if output_dir else Path("./db")
        self.verbose = helix_config.get('verbose', False)

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

        # Node ID mapping: (node_type, kuzu_id) -> helix_id
        self.node_id_mapping = {}

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
            all_tables = self.kuzu_conn.execute("CALL show_tables() RETURN *;").get_as_df()

            if not all_tables.empty:
                node_tables = all_tables[all_tables['type'] == 'NODE']
                for _, row in node_tables.iterrows():
                    table_name = row['name']
                    table_info = self._get_table_properties(table_name)
                    schema["nodes"][table_name] = table_info
                    click.echo(f"  Found node table: {table_name} ({len(table_info['properties'])} properties)")

                rel_tables = all_tables[all_tables['type'] == 'REL']
                for _, row in rel_tables.iterrows():
                    table_name = row['name']
                    table_info = self._get_table_properties(table_name)
                    edge_constraints = self._discover_edge_constraints(table_name)
                    table_info.update(edge_constraints)
                    schema["edges"][table_name] = table_info
                    click.echo(f"  Found edge table: {table_name} FROM {edge_constraints.get('source')} TO {edge_constraints.get('target')} ({len(table_info['properties'])} properties)")

            return schema

        except Exception as e:
            click.echo(f"✗ Schema discovery failed: {e}", err=True)
            raise

    def _get_table_properties(self, table_name: str) -> Dict[str, Any]:
        """Get detailed properties for a Kuzu table."""
        table_info = {
            "properties": {},
            "defaults": {},
            "indexes": []
        }

        try:
            result = self.kuzu_conn.execute(f"CALL table_info('{table_name}') RETURN *;").get_as_df()

            for _, prop in result.iterrows():
                prop_name = prop['name']
                prop_type = prop['type']
                prop_default = prop.get('default expression', None)
                prop_primary_key = prop.get('primary key', False)

                helix_type = self._map_type_to_helix(prop_type)
                table_info["properties"][prop_name] = helix_type

                if prop_primary_key:
                    table_info["indexes"].append(prop_name)
                if prop_default and prop_default != "NULL":
                    table_info["defaults"][prop_name] = prop_default

        except Exception as e:
            click.echo(f"  Warning: Could not get properties for {table_name}: {e}")

        return table_info

    def _discover_edge_constraints(self, edge_table: str) -> Dict[str, Any]:
        """Discover source and target node types for an edge from Kuzu's connection info."""
        try:
            query = f"CALL SHOW_CONNECTION('{edge_table}') RETURN *;"
            result = self.kuzu_conn.execute(query).get_as_df()

            if result.empty:
                click.echo(f"❌ No relationship info found for edge table '{edge_table}'", err=True)
                raise ValueError(f"No relationship info for edge '{edge_table}'")

            src_table = result['source table name'].iloc[0]
            dst_table = result['destination table name'].iloc[0]
            src_pk = result['source table primary key'].iloc[0]
            dst_pk = result['destination table primary key'].iloc[0]

            if not src_table or not dst_table:
                click.echo(
                    f"❌ Invalid relationship metadata for edge '{edge_table}': "
                    f"source={src_table}, target={dst_table}",
                    err=True
                )
                raise ValueError(f"Invalid relationship metadata for edge '{edge_table}'")

            return {
                "source": src_table,
                "target": dst_table,
                "source_key": src_pk,
                "target_key": dst_pk
            }

        except Exception as e:
            click.echo(f"❌ Failed to discover edge constraints for '{edge_table}': {e}", err=True)
            raise RuntimeError(f"Failed to discover edge constraints for '{edge_table}'") from e

    def _map_type_to_helix(self, kuzu_type: str) -> str:
        """Map Kuzu data types to HelixDB types."""
        type_mapping = {
            # Integer types
            'INT128': 'I64',  # HelixDB doesn't have 128-bit signed, map to largest available
            'INT64': 'I64',
            'INT32': 'I32', 
            'INT16': 'I16',
            'INT8': 'I8',
            'SERIAL': 'I64',
            # Unsigned integer types
            'UINT64': 'U64',
            'UINT32': 'U32',
            'UINT16': 'U16',
            'UINT8': 'U8',
            # Floating point types
            'DOUBLE': 'F64',
            'FLOAT': 'F32',
            # String and boolean
            'STRING': 'String',
            'BOOL': 'Boolean',
            # Date/time types
            'DATE': 'Date',
            'TIMESTAMP': 'Date',
            'TIMESTAMP_TZ': 'Date',
            'TIMESTAMP_NS': 'Date', 
            'TIMESTAMP_MS': 'Date',
            'TIMESTAMP_SEC': 'Date',
            'INTERVAL': 'String',
            # Binary and UUID
            'BLOB': 'String',
            'UUID': 'ID',
            # Graph types
            'NODE': 'String',
            'REL': 'String', 
            'RECURSIVE_REL': 'String',
            'INTERNAL_ID': 'ID',
            # Complex types
            'STRUCT': 'String',
            'MAP': 'String',
            'UNION': 'String',
            'LIST': 'String',
            'ARRAY': 'String',
            # Special types
            'ANY': 'String',
            'NULL': 'String',
        }

        # Handle ARRAY types with fixed length, e.g., "INT64[256]" -> "[I64]"
        if '[' in kuzu_type and kuzu_type.endswith(']'):
            # Extract the base type before the brackets
            base_type = kuzu_type.split('[')[0]
            mapped_inner = type_mapping.get(base_type, 'String')
            return f'[{mapped_inner}]'

        # Handle LIST types, e.g., "STRING[]" -> "[String]"
        if kuzu_type.endswith('[]'):
            inner_type = kuzu_type[:-2]
            mapped_inner = type_mapping.get(inner_type, 'String')
            return f'[{mapped_inner}]'

        return type_mapping.get(kuzu_type, 'String')

    def _generate_schema_file(self, kuzu_schema: Dict[str, Any]):
        """Generate schema.hx file using Schema object."""
        click.echo("\n🔨 Generating schema.hx...")

        # Initialize Schema object (this doesn't require HelixDB connection)
        schema = Schema(self.output_dir)

        # Create node types
        for node_name, node_info in kuzu_schema["nodes"].items():
            properties = node_info.get("properties", {})
            indexes = node_info.get("indexes", [])

            click.echo(f"  Adding node type: {node_name}")
            if indexes:
                click.echo(f"    INDEX: {', '.join(indexes)}")

            schema.create_node(node_name, properties, index=indexes)

        # Create edge types
        for edge_name, edge_info in kuzu_schema["edges"].items():
            source_node = edge_info.get("source")
            target_node = edge_info.get("target")

            if not source_node or not target_node:
                raise RuntimeError(
                    f"❌ Edge constraints missing for '{edge_name}': "
                    f"source={source_node}, target={target_node}"
                )

            properties = edge_info.get("properties", {})

            click.echo(f"  Adding edge type: {edge_name}")
            click.echo(f"    FROM {source_node} TO {target_node}")
            if properties:
                click.echo(f"    PROPERTIES: {', '.join(f'{k}:{v}' for k,v in properties.items())}")

            schema.create_edge(edge_name, source_node, target_node, properties)

        # Save schema to file
        schema.save()
        click.echo("✓ Generated schema.hx")

    def generate_queries_file(self, kuzu_schema: Dict[str, Any], output_path: Optional[Path] = None):
        """Generate queries.hx file with HelixQL queries for all nodes and edges."""
        click.echo("\n📝 Generating queries.hx file...")

        if output_path is None:
            output_path = self.output_dir / "queries.hx"

        lines = []
        lines.append("// Auto-generated HelixQL queries for Kuzu to HelixDB migration")
        lines.append(f"// Generated: {datetime.now().isoformat()}")
        lines.append("")

        # Generate node queries
        lines.append("// ============================================")
        lines.append("// Node Creation Queries")
        lines.append("// ============================================")
        lines.append("")

        for node_type, node_info in kuzu_schema["nodes"].items():
            properties = node_info.get("properties", {})

            if properties:
                params = [f"{prop_name}: {prop_type}" for prop_name, prop_type in properties.items()]
                param_str = ", ".join(params)
                prop_assignments = [f"        {prop_name}: {prop_name}" for prop_name in properties.keys()]
                prop_str = ",\n".join(prop_assignments)

                lines.append(f"QUERY Create{node_type} ({param_str}) =>")
                lines.append(f"    node <- AddN<{node_type}>({{")
                lines.append(prop_str)
                lines.append("    })")
                lines.append("    RETURN node")
            else:
                lines.append(f"QUERY Create{node_type} () =>")
                lines.append(f"    node <- AddN<{node_type}>")
                lines.append("    RETURN node")

            lines.append("")

        # Generate edge queries
        lines.append("// ============================================")
        lines.append("// Edge Creation Queries")
        lines.append("// ============================================")
        lines.append("")

        for edge_type, edge_info in kuzu_schema["edges"].items():
            properties = edge_info.get("properties", {})
            source_type = edge_info.get("source", "")
            target_type = edge_info.get("target", "")

            if not source_type or not target_type:
                click.echo(f"  Warning: Missing source/target for edge {edge_type}, skipping query generation")
                continue

            # Ensure variable names are distinct
            if source_type == target_type:
                source_var = f"{source_type.lower()}_from_id"
                target_var = f"{target_type.lower()}_to_id"
            else:
                source_var = f"{source_type.lower()}_id"
                target_var = f"{target_type.lower()}_id"

            # Build parameter list (from_id, to_id, plus properties)
            params = [f"{source_var}: ID", f"{target_var}: ID"]

            if properties:
                params.extend([f"{prop_name}: {prop_type}" for prop_name, prop_type in properties.items()])
                prop_assignments = [f"        {prop_name}: {prop_name}" for prop_name in properties.keys()]
                prop_str = ",\n".join(prop_assignments)

                param_str = ", ".join(params)
                lines.append(f"QUERY Create{edge_type} ({param_str}) =>")
                lines.append(f"    edge <- AddE<{edge_type}>({{")
                lines.append(prop_str)
                lines.append(f"    }})::From({source_var})::To({target_var})")
                lines.append("    RETURN edge")
            else:
                param_str = ", ".join(params)
                lines.append(f"QUERY Create{edge_type} ({param_str}) =>")
                lines.append(f"    edge <- AddE<{edge_type}>")
                lines.append(f"        ::From({source_var})")
                lines.append(f"        ::To({target_var})")
                lines.append("    RETURN edge")

            lines.append("")

        # Write to file
        with open(output_path, 'w') as f:
            f.write("\n".join(lines))

        click.echo(f"✓ Generated queries.hx at {output_path}")
        return output_path

    def export_to_parquet(self, kuzu_schema: Dict[str, Any]):
        """Export Kuzu data to Parquet files for efficient loading."""
        click.echo("\n📦 Exporting data to Parquet format...")

        # Export nodes
        for node_table, node_schema in kuzu_schema["nodes"].items():
            self._export_table_to_parquet(node_table, node_schema, is_node=True)

        # Export edges
        for edge_table, edge_schema in kuzu_schema["edges"].items():
            self._export_table_to_parquet(edge_table, edge_schema, is_node=False)

    def _export_table_to_parquet(self, table_name: str, schema: Dict[str, Any], is_node: bool):
        """Export a Kuzu table to a Parquet file using schema type information."""
        try:
            if is_node:
                query = f"MATCH (n:{table_name}) RETURN n"
            else:
                # Only query the edge - it contains _src and _dst IDs
                query = f"MATCH ()-[r:{table_name}]->() RETURN r"

            result = self.kuzu_conn.execute(query).get_as_df()

            if result.empty:
                click.echo(f"  No data in {'node' if is_node else 'edge'} table: {table_name}")
                return

            processed_data = []
            for _, row in result.iterrows():
                if is_node:
                    record = row['n']
                    if isinstance(record, dict):
                        # Parquet handles type conversion automatically
                        # Just convert special types (datetime, lists, dicts) for compatibility
                        for key, value in list(record.items()):
                            if isinstance(value, (datetime, date, pd.Timestamp)):
                                # Convert to datetime if needed and ensure timezone-aware for RFC 3339
                                if isinstance(value, pd.Timestamp):
                                    value = value.to_pydatetime()
                                if isinstance(value, date) and not isinstance(value, datetime):
                                    value = datetime.combine(value, datetime.min.time())
                                if isinstance(value, datetime) and value.tzinfo is None:
                                    value = value.replace(tzinfo=timezone.utc)
                                record[key] = value.isoformat().replace('+00:00', 'Z')
                            elif isinstance(value, (list, dict)):
                                record[key] = json.dumps(value, default=str)
                        record['_node_type'] = table_name
                        processed_data.append(record)
                else:
                    edge_data = {}
                    rel = row['r']

                    if isinstance(rel, dict):
                        edge_data['_source_id'] = self._extract_id({'_id': rel.get('_src')})
                        edge_data['_target_id'] = self._extract_id({'_id': rel.get('_dst')})
                        edge_data['_source_type'] = schema.get('source', 'Unknown')
                        edge_data['_target_type'] = schema.get('target', 'Unknown')

                        for key, value in rel.items():
                            if key not in ['_src', '_dst', '_id', '_label']:
                                # Parquet handles type conversion automatically
                                # Just convert special types for compatibility
                                if isinstance(value, (datetime, date, pd.Timestamp)):
                                    # Convert to datetime if needed and ensure timezone-aware for RFC 3339
                                    if isinstance(value, pd.Timestamp):
                                        value = value.to_pydatetime()
                                    if isinstance(value, date) and not isinstance(value, datetime):
                                        value = datetime.combine(value, datetime.min.time())
                                    if isinstance(value, datetime) and value.tzinfo is None:
                                        value = value.replace(tzinfo=timezone.utc)
                                    edge_data[key] = value.isoformat().replace('+00:00', 'Z')
                                elif isinstance(value, (list, dict)):
                                    edge_data[key] = json.dumps(value, default=str)
                                else:
                                    edge_data[key] = value

                    edge_data['_edge_type'] = table_name
                    processed_data.append(edge_data)

            if processed_data:
                df = pd.DataFrame(processed_data)
                file_suffix = "nodes" if is_node else "edges"
                output_file = self.parquet_dir / f"{table_name}_{file_suffix}.parquet"

                table = pa.Table.from_pandas(df)
                pq.write_table(table, output_file)

                count = len(df)
                click.echo(f"  ✓ Exported {count} {'nodes' if is_node else 'edges'} from {table_name} to {output_file.name}")

                if is_node:
                    self.stats["nodes_migrated"][table_name] = count
                    self.stats["total_nodes"] += count
                else:
                    self.stats["edges_migrated"][table_name] = count
                    self.stats["total_edges"] += count

        except Exception as e:
            error_msg = f"Failed to export table {table_name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)

    def import_to_helix(self):
        """Import Parquet files to HelixDB."""
        click.echo("\n📥 Importing data to HelixDB...")

        # Import nodes first
        node_files = list(self.parquet_dir.glob("*_nodes.parquet"))
        for node_file in tqdm(node_files, desc="Importing nodes"):
            self._import_node_file(node_file)

        # Check if any node mappings were created
        if not self.node_id_mapping:
            raise Exception(
                "\n❌ No node ID mappings created! This means queries failed.\n"
                "   Did you run 'HelixDB push dev' to deploy the schema?\n"
                "   Check the error messages above for details."
            )

        click.echo(f"\n✓ Created {len(self.node_id_mapping)} node ID mappings")

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
            query_name = f'Create{node_type}'

            # Batch import
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]

                for _, row in batch.iterrows():
                    # Convert row to dict
                    data = row.to_dict()

                    # Extract Kuzu ID for mapping (already a JSON string from Parquet)
                    kuzu_id = data.get('_id')

                    # Remove internal fields before sending to Helix
                    data.pop('_node_type', None)
                    data.pop('_id', None)
                    data.pop('_label', None)

                    # Deserialize JSON strings and handle nulls
                    data = self._deserialize_row(data)

                    # Execute string-based query (uses queries.hx)
                    response = self.helix_client.query(query_name, data)

                    # Check if query failed
                    if not response or response == [None]:
                        raise Exception(f"Query Create{node_type} failed - check if schema is deployed with 'HelixDB push dev'")

                    # Extract HelixDB ID and store mapping
                    if response and len(response) > 0 and kuzu_id is not None:
                        helix_id = self._extract_helix_id(response[0])
                        if helix_id:
                            self.node_id_mapping[(node_type, kuzu_id)] = helix_id
                            if self.verbose:
                                click.echo(f"      Stored mapping: ({node_type}, {kuzu_id}) -> {helix_id}")
                        else:
                            raise Exception(f"Could not extract HelixDB ID from response: {response[0]}")

        except Exception as e:
            error_msg = f"Failed to import {parquet_file.name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)

    def _import_edge_file(self, parquet_file: Path):
        """Import edges from a Parquet file."""
        try:
            table = pq.read_table(parquet_file)
            df = table.to_pandas()

            if df.empty:
                return

            edge_type = parquet_file.stem.replace('_edges', '')
            query_name = f'Create{edge_type}'

            skipped_count = 0
            batch_size = 100
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i+batch_size]
                for _, row in batch.iterrows():
                    kuzu_source_id = row['_source_id']
                    kuzu_target_id = row['_target_id']
                    source_type = row['_source_type']
                    target_type = row['_target_type']

                    helix_source_id = self.node_id_mapping.get((source_type, kuzu_source_id))
                    helix_target_id = self.node_id_mapping.get((target_type, kuzu_target_id))

                    if not helix_source_id or not helix_target_id:
                        if self.verbose:
                            click.echo(f"      Missing mapping:")
                            click.echo(f"        Looking for: ({source_type}, {repr(kuzu_source_id)}) -> {helix_source_id}")
                            click.echo(f"        Looking for: ({target_type}, {repr(kuzu_target_id)}) -> {helix_target_id}")
                            click.echo(f"        Available: {list(self.node_id_mapping.keys())[:3]}")
                        skipped_count += 1
                        continue

                    data = row.to_dict()
                    for key in ['_source_id', '_target_id', '_source_type', '_target_type', '_edge_type', '_id', '_label']:
                        data.pop(key, None)

                    data = self._deserialize_row(data)

                    # Build parameters for edge query based on generated queries.hx
                    # Edge queries expect source/target IDs with specific parameter names
                    if source_type == target_type:
                        source_param = f"{source_type.lower()}_from_id"
                        target_param = f"{target_type.lower()}_to_id"
                    else:
                        source_param = f"{source_type.lower()}_id"
                        target_param = f"{target_type.lower()}_id"

                    query_params = {
                        source_param: helix_source_id,
                        target_param: helix_target_id,
                        **data
                    }

                    # Execute string-based query (uses queries.hx)
                    self.helix_client.query(query_name, query_params)

            if skipped_count > 0:
                click.echo(f"  ⚠️  Skipped {skipped_count} edges due to missing node ID mappings")

        except Exception as e:
            error_msg = f"Failed to import {parquet_file.name}: {e}"
            click.echo(f"  ✗ {error_msg}")
            self.stats["errors"].append(error_msg)

    def _extract_id(self, node: Dict) -> str:
        """
        Extract ID from a Kuzu node and return as JSON string.
        Kuzu IDs have format: {'offset': int, 'table': int}
        Returns: '{"offset": 0, "table": 4}' (JSON string, hashable)
        """
        node_id = node.get('_id')
        if node_id is None:
            return None
        return json.dumps(node_id, sort_keys=True, default=str)

    def _extract_helix_id(self, response: Any) -> Optional[Any]:
        """Extract HelixDB node ID from response."""
        if isinstance(response, dict):
            if 'node' in response and isinstance(response['node'], dict):
                node_data = response['node']
                if 'id' in node_data:
                    return node_data['id']
        return None

    def _deserialize_row(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """Deserialize a row from Parquet, converting JSON strings back to objects."""
        for key, value in list(row_data.items()):
            if pd.isna(value):
                row_data[key] = None
            elif isinstance(value, str) and value.startswith(('[', '{')):
                try:
                    row_data[key] = json.loads(value)
                except json.JSONDecodeError:
                    pass  # Keep as string if not valid JSON
        return row_data

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

    def run(self, skip_deploy_prompt: bool = False):
        """Execute the complete migration."""
        click.echo("\n" + "="*60)
        click.echo("Kuzu to HelixDB Migration")
        click.echo("="*60)

        try:
            # Step 1: Connect to Kuzu and discover schema
            self.connect_kuzu()
            kuzu_schema = self.discover_kuzu_schema()

            # Step 2: Clear existing schema and query files
            schema_path = self.output_dir / "schema.hx"
            queries_path = self.output_dir / "queries.hx"

            if schema_path.exists():
                schema_path.unlink()
                click.echo(f"🗑️  Cleared existing {schema_path}")
            if queries_path.exists():
                queries_path.unlink()
                click.echo(f"🗑️  Cleared existing {queries_path}")

            # Step 3: Generate schema.hx
            self._generate_schema_file(kuzu_schema)

            # Step 4: Generate queries.hx file
            queries_path = self.generate_queries_file(kuzu_schema)
            click.echo(f"✓ Queries file: {queries_path}")

            # Step 5: Prompt user to deploy schema to Helix
            if not skip_deploy_prompt:
                click.echo("\n" + "="*60)
                click.echo("📤 Deploy Schema to HelixDB")
                click.echo("="*60)
                click.echo("\nPlease run the following command to deploy your schema:")
                click.echo("\n    HelixDB push dev")
                click.echo("\n(or 'HelixDB push prod' for production)")
                click.echo("\nPress Enter once deployment is complete...")
                input()

            # Step 6: Connect to HelixDB client (after deployment)
            self.connect_helix()

            # Step 7: Export data to Parquet
            self.export_to_parquet(kuzu_schema)

            # Step 8: Import data from Parquet to Helix
            self.import_to_helix()

            # Step 9: Generate report
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
@click.option('--skip-deploy', is_flag=True, help='Skip the deployment prompt')
def main(kuzu_path, helix_port, helix_endpoint, local, output, verbose, skip_deploy):
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
    migrator.run(skip_deploy_prompt=skip_deploy)


if __name__ == "__main__":
    main()
