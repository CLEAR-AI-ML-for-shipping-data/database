import os

import psycopg2
import geopandas
from sqlalchemy import create_engine

from psycopg2 import sql
from configparser import ConfigParser
from psycopg2 import OperationalError

class PostGISConnection:
	def __init__(self, config_path=None, section='postgres', schema='public', db_config=None):
		if db_config:
			self.db_config = db_config
		else:
			# locate config file next to this script if none provided
			config_path = config_path or os.path.join(os.path.dirname(__file__), 'config.ini')
			parser = ConfigParser()
			parser.read(config_path)

			if not parser.has_section(section):
				raise KeyError(f"Section {section!r} not found in {config_path}")
			
			cfg = parser[section]

			self.db_config = {
				'host':     cfg.get('host'),
				'port':     cfg.get('port',   fallback='5432'),
				'dbname':   cfg.get('dbname'),
				'user':     cfg.get('user'),
				'password': cfg.get('password'),
			}
		self._conn = None
		self.schema = schema

	def test_db_connection(self, timeout=5):

		db_config = self.db_config

		# Ensure we have a connect_timeout parameter
		if 'connect_timeout' not in db_config:
			db_config['connect_timeout'] = timeout

		try:
			self._conn = psycopg2.connect(**self.db_config)
			return True, "OK"

		except OperationalError as e:

			msg = str(e)
			if 'timeout' in msg.lower():
				return False, f"Connection timed out ({timeout}s)"

			return False, msg

	def connect(self):

		if self._conn is None or self._conn.closed:
			self._conn = psycopg2.connect(**self.db_config)

		return self._conn

	def _ensure_conn(self):

		"""Open connection if not already open or if it’s closed."""

		if self._conn is None or self._conn.closed:
			self._conn = psycopg2.connect(**self.db_config)

		return self._conn

	def close(self):

		if self._conn and not self._conn.closed:
			self._conn.close()
			self._conn = None

	def fetchone(self, sql, params=None):

		cur = self._ensure_conn().cursor()
		cur.execute(sql, params or ())
		row = cur.fetchone()
		cur.close()

		return row

	def fetchall(self, sql, params=None):

		cur = self._ensure_conn().cursor()
		cur.execute(sql, params or ())
		rows = cur.fetchall()
		cur.close()

		return rows

	def list_trajectory_tables(self) -> list[str]:

		"""
		List all trajectory tables in the schema, e.g. 'trajectories_2023_01'.
		"""

		query = """
			SELECT tablename FROM pg_tables
			WHERE schemaname = %s AND tablename LIKE %s;
		"""

		stmt = sql.SQL(query)

		rows = self.fetchall(stmt, (self.schema, 'trajectories_%'))
		return [r[0] for r in rows]

	def create_embedding_table(self,
		table_name: str, 
		embedding_length: int = 32,
		referenced_table: str = "trajectories",
		referenced_column: str = "id",
		on_delete: str = "CASCADE"
	):
		
		"""
		Create a table with HNSW index if it does not exist.
		trajectory_id will be a foreign key referencing referenced_table(referenced_column).
		on_delete controls the FK action (e.g. 'CASCADE', 'SET NULL', 'RESTRICT', 'NO ACTION').
		"""
		
		# Validate simple options (avoid SQL injection via these limited strings)
		if on_delete not in ("CASCADE", "SET NULL", "RESTRICT", "NO ACTION"):
			raise ValueError("on_delete must be one of: CASCADE, SET NULL, RESTRICT, NO ACTION")

		"""
		Create a table with HNSW index if it does not exist.
		"""
		# SQL for creating the table and setting hnsw.ef_search
		create_table_and_set_sql = """
			CREATE TABLE IF NOT EXISTS {table_name} (
				id            BIGSERIAL 						PRIMARY KEY,
				mmsi          VARCHAR        					NOT NULL,
				trajectory_id BIGINT                             NOT NULL
                	REFERENCES {ref_table}({ref_col}) ON DELETE {on_delete},
				start_dt      TIMESTAMP WITHOUT TIME ZONE    	NOT NULL,
				end_dt        TIMESTAMP WITHOUT TIME ZONE    	NOT NULL,
				embedding     vector({embedding_length})     	NOT NULL
			);
			CREATE INDEX IF NOT EXISTS {mmsi_traj_idx_name}
			ON {table_name} (mmsi, trajectory_id);
			SET hnsw.ef_search = 100;
		"""

		# SQL for creating the HNSW index concurrently
		create_index_sql = """
			CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
				ON {table_name} USING hnsw (embedding vector_cosine_ops)
			WITH (m = 16, ef_construction = 256);
		"""
		
		conn = self._ensure_conn()

		try:
			# Execute CREATE TABLE and SET hnsw.ef_search within a transaction
			with conn.cursor() as cur:
				stmt_table = sql.SQL(create_table_and_set_sql).format(
					table_name=sql.Identifier(table_name),
					ref_table=sql.Identifier(referenced_table),
					ref_col=sql.Identifier(referenced_column),
					on_delete=sql.SQL(on_delete),
					embedding_length=sql.Literal(embedding_length),
					mmsi_traj_idx_name=sql.Identifier(f"{table_name}_mmsi_trajectory_id_idx")
				)
				cur.execute(stmt_table)
			conn.commit()

			# For CREATE INDEX CONCURRENTLY, temporarily set autocommit to True
			# because it cannot run inside a transaction block.
			old_autocommit = conn.autocommit
			conn.autocommit = True
			with conn.cursor() as cur:
				stmt_index = sql.SQL(create_index_sql).format(
					table_name=sql.Identifier(table_name),
					index_name=sql.Identifier(f"{table_name}_hnsw")
				)
				cur.execute(stmt_index)
			conn.autocommit = old_autocommit # Restore autocommit setting

		except Exception as e:
			conn.rollback() # Rollback in case of any error during table creation or index creation
			raise e
		finally:
			if conn.autocommit: # Ensure autocommit is restored
				conn.autocommit = old_autocommit

	def entry_exists(self, table_name: str, mmsi: str, trajectory_id: int, start_dt, end_dt) -> bool:
		"""
		Checks if an entry with the given mmsi, trajectory_id, start_dt, and end_dt exists in the table.
		"""
		conn = self._ensure_conn()
		cur = conn.cursor()
		try:
			select_sql = """
				SELECT id FROM {table_name}
				WHERE mmsi = %s AND trajectory_id = %s AND start_dt = %s AND end_dt = %s;
			"""
			stmt_select = sql.SQL(select_sql).format(table_name=sql.Identifier(table_name))
			cur.execute(stmt_select, (mmsi, trajectory_id, start_dt, end_dt))
			return cur.fetchone() is not None
		finally:
			cur.close()

	def get_existing_keys(self, table_name: str, keys: list) -> set:
		"""
		Given a list of (mmsi, trajectory_id, start_dt, end_dt) tuples, return
		the subset that already exists in the table — using a single query.
		"""
		if not keys:
			return set()

		conn = self._ensure_conn()
		cur = conn.cursor()
		try:
			# Build a VALUES list so we can do a single round-trip.
			values_placeholder = ','.join(['(%s,%s,%s,%s)'] * len(keys))
			flat_params = [v for k in keys for v in k]

			query = sql.SQL("""
				SELECT t.mmsi, t.trajectory_id, t.start_dt, t.end_dt
				FROM {table_name} AS t
				JOIN (VALUES {values}) AS v(mmsi, trajectory_id, start_dt, end_dt)
				  ON t.mmsi = v.mmsi
				 AND t.trajectory_id = v.trajectory_id::bigint
				 AND t.start_dt = v.start_dt::timestamp
				 AND t.end_dt   = v.end_dt::timestamp;
			""").format(
				table_name=sql.Identifier(table_name),
				values=sql.SQL(values_placeholder)
			)
			cur.execute(query, flat_params)
			rows = cur.fetchall()
			return {(str(r[0]), int(r[1]), r[2], r[3]) for r in rows}
		finally:
			cur.close()

	def insert_embedding_data(self, table_name: str, mmsi: str, trajectory_id: int, 
	                              start_dt, end_dt, embedding, update_entry: bool = False):
		"""
		Inserts or updates embedding data into the specified table.

		Args:
			table_name (str): The name of the table.
			mmsi (str): The MMSI of the entry.
			trajectory_id (int): The trajectory ID of the entry.
			start_dt: The start datetime of the entry.
			end_dt: The end datetime of the entry.
			embedding: The embedding vector.
			update_entry (bool): If True, updates the entry if it exists; otherwise, skips if exists.
		"""
		conn = self._ensure_conn()
		cur = conn.cursor()

		try:

			if update_entry:
				# Update existing entry
				update_sql = """
					UPDATE {table_name} SET embedding = %s
					WHERE mmsi = %s AND trajectory_id = %s AND start_dt = %s AND end_dt = %s;
				"""
				stmt_update = sql.SQL(update_sql).format(table_name=sql.Identifier(table_name))
				cur.execute(stmt_update, (embedding, mmsi, trajectory_id, start_dt, end_dt))
				conn.commit()
			else:
				# Insert new entry
				insert_sql = """
					INSERT INTO {table_name} (mmsi, trajectory_id, start_dt, end_dt, embedding)
					VALUES (%s, %s, %s, %s, %s);
				"""
				stmt_insert = sql.SQL(insert_sql).format(table_name=sql.Identifier(table_name))
				cur.execute(stmt_insert, (mmsi, trajectory_id, start_dt, end_dt, embedding))
				conn.commit()

		except Exception as e:
			conn.rollback()
			raise e
		finally:
			cur.close()


	def count_table(self, table_name: str, ship_type: int = None) -> int:

		"""Return row count for any table in the schema."""

		if ship_type == None:

			query = """
				SELECT COUNT(*) FROM {sch}.{tbl};
			"""

			stmt = sql.SQL(query).format(
				sch=sql.Identifier(self.schema),
				tbl=sql.Identifier(table_name)
			)

			row = self.fetchone(stmt, params=(ship_type,))

		else:

			query = """
				SELECT COUNT(*)
				FROM {sch}.{tbl} AS t
				JOIN {sch}.ships AS s ON t.mmsi = s.mmsi
				WHERE s.type_of_ship = %s
			"""

			stmt = sql.SQL(query).format(
				sch=sql.Identifier(self.schema),
				tbl=sql.Identifier(table_name)
			)

			row = self.fetchone(stmt, params=(ship_type,))

		return row[0] if row else 0

	def load_dataframe_chunked(self, table_name: str, chunksize: int, ship_type: int = None):
		"""
		Read a PostgreSQL table into GeoDataFrames in chunks using geopandas.
		
		Args:
			table_name (str): The name of the table to read.
			chunksize (int): The number of rows to fetch per chunk.

		Yields:
			geopandas.GeoDataFrame: A chunk of the table as a GeoDataFrame.
		"""
		# Create a SQLAlchemy engine from the db_config
		db_url = f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
		engine = create_engine(db_url)

		cols = [
			't.id',
			't.mmsi',
			't.start_dt',
			't.end_dt',
			't.coordinates',
			't.timestamps',
			't.heading',
			't.course_over_ground'
		]

		#query = f'SELECT {",".join(cols)} FROM {self.schema}.{table_name} ORDER BY id ASC LIMIT 5'

		if ship_type == None:
			query = f'SELECT {",".join(cols)} FROM {self.schema}.{table_name} as t ORDER BY id'
		else:
			from_query = f'SELECT {",".join(cols)} FROM {self.schema}.{table_name} as t'
			join_query = f'JOIN {self.schema}.ships as s ON t.mmsi = s.mmsi'
			filter_query = f'WHERE s.type_of_ship = {ship_type}'
			query = f'{from_query} {join_query} {filter_query} ORDER BY id'

		return geopandas.read_postgis(
			query,
			engine,
			geom_col='coordinates',
			chunksize=chunksize
		)

if __name__ == '__main__':

	db = PostGISConnection()
	traj_tables = db.list_trajectory_tables()
	traj_tables.sort()

	print('\nTesting database connection\n')

	for table in traj_tables:
		print(f'{table}: {db.count_table(table)} rows')
		
