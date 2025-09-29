import logging
import psycopg2
from typing import Dict, Iterable, List, Sequence
import sys

try:  # optional imports for postgres
	import psycopg2
	from psycopg2.extras import RealDictCursor  # type: ignore
except Exception:  # pragma: no cover
	psycopg2 = None  # type: ignore
	RealDictCursor = None  # type: ignore

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s - %(message)s')
# ----------------------------------------------------------------------------
# Database Extraction (Generic)
# ----------------------------------------------------------------------------

def connect_db(source_cfg: Dict):
	"""Return a live DB connection and normalized db_type.

	Supports: postgresql/postgres, mysql/mariadb, mssql/sqlserver.
	Additional drivers imported lazily.
	"""
	db_type = (source_cfg.get('db_type') or 'postgresql').lower()
	sec_cfg = source_cfg.get('sec_config', {}) or {}
	host = source_cfg.get('host')
	port = source_cfg.get('port')
	database = source_cfg.get('database')

	if db_type in ('postgresql', 'postgres'):
		if psycopg2 is None:
			raise RuntimeError(
                'psycopg2 not installed. Add psycopg2-binary '
                'to requirements.txt')
		user = sec_cfg.get('user') 
		password = sec_cfg.get('password') 
		logger.info('Connecting to PostgreSQL host=%s db=%s', host, database)
		conn = psycopg2.connect(
			host=host,
			port=port or 5432,
			dbname=database,
			user=user,
			password=password,
			connect_timeout=10,
		)
		return conn, 'postgresql'

	if db_type in ('mysql', 'mariadb'):
        # lazy import only if needed
		try:
			import pymysql  # type: ignore
		except ImportError as e:  
			raise RuntimeError(
       'PyMySQL not installed. Add PyMySQL to requirements.txt') from e
		user = sec_cfg.get('user') 
		password = sec_cfg.get('password')
		logger.info('Connecting to MySQL host=%s db=%s', host, database)
		conn = pymysql.connect(
			host=host,
			port=int(port or 3306),
			db=database,
			user=user,
			password=password,
			charset='utf8mb4',
			cursorclass=pymysql.cursors.Cursor,
		)
		return conn, 'mysql'

	if db_type in ('mssql', 'sqlserver', 'sql_server'):
		try:
			import pyodbc  # type: ignore
		except ImportError as e:  # pragma: no cover
			raise RuntimeError('pyodbc not installed. Add pyodbc to requirements.txt') from e
		user = sec_cfg.get('user') 
		password = sec_cfg.get('password') 
		driver = sec_cfg.get('driver')
  
		logger.info('Connecting to SQL Server host=%s db=%s', host, database)
		conn_str = f'DRIVER={{{driver}}};SERVER={host},{port or 1433};DATABASE={database};UID={user};PWD={password}'
		conn = pyodbc.connect(conn_str, timeout=10)
		return conn, 'mssql'

	raise ValueError(f'Unsupported db_type: {db_type}')


def _quote_identifier(col: str, db_type: str) -> str:
	if db_type == 'postgresql':
		return f'"{col}"'
	if db_type == 'mysql':
		return f'`{col}`'
	if db_type == 'mssql':
		return f'[{col}]'
	return col


def fetch_batches(
	conn,
	table_path: str,
	columns: Sequence[str],
	db_type: str,
	fetch_size: int = 10_000,
) -> Iterable[List[Dict]]:
	"""Generator yielding batches of rows as list[dict] generically."""
	quoted_cols = ', '.join([_quote_identifier(c, db_type) for c in columns])
	sql = f'SELECT {quoted_cols} FROM {table_path}'
	logger.info('Executing query: %s', sql)

	cursor_kwargs = {}
	use_dict_cursor = False
	if db_type == 'postgresql' and psycopg2 is not None and RealDictCursor is not None:
		cursor_kwargs['cursor_factory'] = RealDictCursor  # type: ignore
		use_dict_cursor = True

	with conn.cursor(**cursor_kwargs) as cur:  # type: ignore
		try:
			if hasattr(cur, 'itersize'):
				cur.itersize = fetch_size  # streaming optimization (postgres)
		except Exception:  # pragma: no cover
			pass
		cur.execute(sql)
		col_names = None
		while True:
			rows = cur.fetchmany(fetch_size)
			if not rows:
				break
			if use_dict_cursor:
				yield rows
			else:
				if col_names is None:
					col_names = [d[0] for d in cur.description]
				yield [dict(zip(col_names, r)) for r in rows]