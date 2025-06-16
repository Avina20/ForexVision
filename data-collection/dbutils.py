import os
from pymongo import MongoClient, server_api
import sqlite3
from datetime import datetime
from threading import Lock

class DB_Utils(object):
    """
    Enhanced Database utilities class for currency pair classification project.
    
    Methods
    -------
    push_to_sqlite_db(data)
        Pushes data into SQLite DB with proper schema.
        
    push_to_mongo_db(data)
        Pushes data into MongoDB Atlas with proper schema.
    """
    
    def __init__(self, db_name, collection_name, mode, mongo_uri=None):
        """
        Initialize databases based on mode (auxiliary or main).
        
        Parameters
        ----------
        db_name: str
            Database name.
        collection_name: str
            Collection/table name.
        mode: str
            'aux' for auxiliary or 'main' for main database.
        mongo_uri: str
            MongoDB Atlas connection URI.
        """
        self.db_name = db_name
        self.collection_name = collection_name
        self.mode = mode
        self.sql_lock = Lock()  # Thread lock for SQLite
        self.mongo_lock = Lock()
        
        self.__set_mode_specifics()
        self.__create_sqlite_db()
        self.__create_mongo_db(mongo_uri)
    
    def __set_mode_specifics(self):
        """Sets schema definitions based on database mode."""
        if self.mode == 'aux':
            # Auxiliary database schema (hourly data)
            self.sql_schema = f'''CREATE TABLE IF NOT EXISTS {self.collection_name} (
                                timestamp TEXT NOT NULL,
                                currency TEXT NOT NULL,
                                rate REAL NOT NULL,
                                PRIMARY KEY (timestamp, currency))'''
            
            self.sql_insert_cmd = f"""INSERT INTO {self.collection_name} 
                                    (timestamp, currency, rate)
                                    VALUES (?, ?, ?)""" 
            
            self.mongo_data_dict = {
                "timestamp": None,
                "currency": None,
                "rate": None,
            }
            
        elif self.mode == 'main':
            # Main database schema (6-hour features)
            self.sql_schema = f'''CREATE TABLE IF NOT EXISTS {self.collection_name} (
                                timestamp TEXT NOT NULL,
                                currency TEXT NOT NULL,
                                max REAL NOT NULL,
                                min REAL NOT NULL,
                                max_min REAL NOT NULL,
                                mean REAL NOT NULL,
                                corr_btc REAL,
                                norm_vol REAL NOT NULL,
                                fd REAL NOT NULL,
                                insert_timestamp TEXT NOT NULL,
                                PRIMARY KEY (timestamp))'''
            
            self.sql_insert_cmd = f"""INSERT INTO {self.collection_name} 
                                    (timestamp, currency, max, min, max_min, mean,
                                    corr_btc, norm_vol, fd, insert_timestamp)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                                
            
            self.mongo_data_dict = {
                "timestamp": None,
                "currency": None,
                "max": None,
                "min": None,
                "max_min": None,
                "mean": None,
                "corr_btc":None,
                "norm_vol": None,
                "fd": None,
                "insert_timestamp": None,
            }
    
    def __create_sqlite_db(self):
        """Creates SQLite database with specified schema."""
        self.conn = sqlite3.connect(f'{self.db_name}.db',check_same_thread=False, timeout=30.0)
        self.cursor = self.conn.cursor()
        self.cursor.execute(self.sql_schema)
        self.cursor.execute("PRAGMA journal_mode = WAL")
        self.cursor.execute("PRAGMA synchronous = NORMAL")
        
    def __create_mongo_db(self, mongo_uri):
        """Creates MongoDB connection with specified schema."""
        if mongo_uri:
            try:
                self.client = MongoClient(mongo_uri, server_api=server_api.ServerApi('1'), connect=False, serverSelectionTimeoutMS=5000,     socketTimeoutMS=30000, connectTimeoutMS=30000)
                self.client.admin.command('ping')
                print("Successfully connected to MongoDB Atlas!")
                
                self.db = self.client[self.db_name]
                self.collection = self.db[self.collection_name]

            except Exception as e:
                print(f"MongoDB connection error: {e}")
                raise
        else:
            raise ValueError("MongoDB URI is required for Atlas connection")
    
    def push_to_sqlite_db(self, data):
        """Inserts data into SQLite database."""
        try:
            self.cursor.execute(self.sql_insert_cmd, data)
            self.conn.commit()
        except sqlite3.IntegrityError:
            print(f"Duplicate entry detected: {data[0]}, {data[1]}")
        except Exception as e:
            print(f"SQLite insert error: {e}")
    
    def push_to_mongo_db(self, data):
        """Inserts data into MongoDB."""
        try:
            document = {key: value for key, value in zip(self.mongo_data_dict.keys(), data)}
            self.collection.insert_one(document)
        except Exception as e:
            print(f"MongoDB insert error: {e}")

    def bulk_insert_sqlite(self, records):
        """Thread-safe bulk insert for SQLite"""
        with self.sql_lock:
            try:
                self.cursor.executemany(self.sql_insert_cmd, records)
                self.conn.commit()
                return True
            except Exception as e:
                print(f"SQLite bulk insert error: {e}")
                self.conn.rollback()
                return False
    
    def bulk_insert_mongo(self, documents):
        """Thread-safe bulk insert for MongoDB"""
        with self.mongo_lock:
            try:
                if documents:
                    self.collection.insert_many(documents, ordered=False)
                return True
            except Exception as e:
                print(f"MongoDB bulk insert error: {e}")
                return False
    
    def delete_sqlite_db(self):
        """Deletes SQLite database file."""
        self.cursor.close()
        self.conn.close()
        try:
            os.remove(f'{self.db_name}.db')
        except FileNotFoundError:
            print("SQLite database file not found.")
    
    def delete_mongo_db(self):
        """Drops MongoDB collection."""
        try:
            self.collection.drop()
            print(f"Collection {self.collection_name} dropped successfully.")
        except Exception as e:
            print(f"Error dropping collection: {e}")

    def find_records(self, query=None, sort=None, projection=None, limit=None):
        """
        Unified find method that works for both SQLite and MongoDB
        
        Parameters:
        - query: Dict for MongoDB, WHERE clause string for SQLite
        - sort: List of tuples [("field", direction)] for both
                (1/-1 for MongoDB, "ASC"/"DESC" for SQLite)
        - projection: Dict for MongoDB, list of columns for SQLite
        - limit: Maximum number of records to return
        
        Returns:
        - List of records (as dicts for MongoDB, tuples for SQLite)
        """
        if self.mode == 'aux' or self.mode == 'main':
            if hasattr(self, 'collection'):  # MongoDB
                return self._find_mongo(query, sort, projection, limit)
            else:  # SQLite
                return self._find_sqlite(query, sort, projection, limit)
        else:
            raise ValueError("Invalid database mode")

    def _find_mongo(self, query, sort, projection, limit):
        """MongoDB-specific find implementation"""
        try:
            if query is None:
                query = {}
            
            cursor = self.collection.find(query, projection)
            
            if sort:
                cursor = cursor.sort(sort)
                
            if limit:
                cursor = cursor.limit(limit)
                
            return list(cursor)
        except Exception as e:
            print(f"MongoDB find error: {e}")
            return []
    
    def _find_sqlite(self, query, sort, projection, limit):
        """SQLite-specific find implementation"""
        try:
            # Build SELECT clause
            if projection:
                columns = ', '.join(projection)
            else:
                columns = '*'
            
            # Build WHERE clause
            where = ''
            if query:
                where = f"WHERE {query}"
            
            # Build ORDER BY clause
            order_by = ''
            if sort:
                order_by_parts = []
                for field, direction in sort:
                    dir_str = "DESC" if direction == -1 or direction == "DESC" else "ASC"
                    order_by_parts.append(f"{field} {dir_str}")
                order_by = "ORDER BY " + ", ".join(order_by_parts)
            
            # Build LIMIT clause
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            # Execute query
            query_str = f"SELECT {columns} FROM {self.collection_name} {where} {order_by} {limit_clause}"
            self.cursor.execute(query_str)
            
            # Get column names
            columns = [desc[0] for desc in self.cursor.description]
            
            # Convert to list of dicts for consistency
            return [dict(zip(columns, row)) for row in self.cursor.fetchall()]
            
        except Exception as e:
            print(f"SQLite find error: {e}")
            return []