import logging
import cx_Oracle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class OracleMigrationTool:
    def __init__(self, username, password, dsn):
        self.username = username
        self.password = password
        self.dsn = dsn
        self.connection = None

    def connect(self):
        try:
            self.connection = cx_Oracle.connect(self.username, self.password, self.dsn)
            logging.info('Connection established successfully.')
        except cx_Oracle.DatabaseError as e:
            logging.error(f'Error connecting to the database: {e}')
            raise

    def migrate(self, sql_commands):
        if self.connection is None:
            logging.error('No database connection. Please connect first.')
            return
        
        try:
            cursor = self.connection.cursor()
            for command in sql_commands:
                logging.info(f'Executing command: {command}')
                cursor.execute(command)
            self.connection.commit()
            logging.info('Migration completed successfully.')
        except Exception as e:
            logging.error(f'Error during migration: {e}')
            self.connection.rollback()
            logging.info('Transaction rolled back.')
        finally:
            cursor.close()

    def disconnect(self):
        if self.connection:
            self.connection.close()
            logging.info('Connection closed.')

# Usage
if __name__ == '__main__':
    # Define your Oracle DB credentials and DSN
    USERNAME = 'your_username'
    PASSWORD = 'your_password'
    DSN = 'your_dsn'
    
    migration_tool = OracleMigrationTool(USERNAME, PASSWORD, DSN)
    migration_tool.connect()
    
    # Define your SQL commands for migration
    sql_commands = [
        "CREATE TABLE example_table (id NUMBER PRIMARY KEY, name VARCHAR2(50))",
        "INSERT INTO example_table (id, name) VALUES (1, 'Sample Name')"
    ]
    
    migration_tool.migrate(sql_commands)
    migration_tool.disconnect()