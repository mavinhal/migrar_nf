from oracle_migration_tool import DatabaseMigrator
from config import *
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler('migration.log'), logging.StreamHandler()])
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting Oracle Database Migration...")
    source_config = {'user': SOURCE_DATABASE_USER, 'password': SOURCE_DATABASE_PASSWORD, 'dsn': f'{SOURCE_DATABASE_HOST}:{SOURCE_DATABASE_PORT}/{SOURCE_DATABASE_SID}'}
    target_config = {'user': TARGET_DATABASE_USER, 'password': TARGET_DATABASE_PASSWORD, 'dsn': f'{TARGET_DATABASE_HOST}:{TARGET_DATABASE_PORT}/{TARGET_DATABASE_SID}'}
    
    try:
        migrator = DatabaseMigrator(source_config, target_config, batch_size=MIGRATION_BATCH_SIZE)
        logger.info(f"DatabaseMigrator initialized with batch size: {MIGRATION_BATCH_SIZE}")

        tables_to_migrate = [('TB_NF', 'TB_NF'), ('TB_NF_ITEM', 'TB_NF_ITEM')]
        if not tables_to_migrate:
            logger.warning("No tables configured for migration. Update the tables_to_migrate list")
            return

        for source_table, target_table in tables_to_migrate:
            logger.info(f"Starting migration of table: {source_table} -> {target_table}")
            try:
                migrator.migrate_table(source_table, target_table)
                logger.info(f"Table {source_table} data migration completed")
                migrator.validate_migration(source_table, target_table)
                logger.info(f"✅ Table {source_table} migrated and validated successfully!")
            except Exception as e:
                logger.error(f"❌ Error migrating table {source_table}: {str(e)}")
                continue
        logger.info("Oracle Database Migration completed successfully!")
    except Exception as e:
        logger.error(f"Fatal error during migration: {str(e)}")
        raise

if __name__ == '__main__':
    main()
