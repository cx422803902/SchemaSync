import logging
import optparse
import schemaobject
import utils

import syncdb

from schemaobject.connection import DatabaseConnection as BaseConnection

def extOptions(parser):
    parser.add_option("--createdb",
                            dest="create_datebase_name",
                            help="create datebase in target mysql")
    parser.add_option("--deletedb",
                        dest="delete_datebase_name",
                        help="delete datebase in target mysql")
    parser.add_option("--synchronizedTables",
                            dest="synchronizedTables",
                            action="store_true",
                            default=False,
                            help="synchronized the tables from source to target")                
    parser.add_option("--executeSql",
                            dest="executeSql",
                            help"execute sql file")

def extApp(options, sourcedb='', targetdb=''):
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)    

    source_info = schemaobject.connection.parse_database_url(sourcedb)
    target_info = schemaobject.connection.parse_database_url(targetdb)
    if not source_info:            
        logging.error("Invalid source database URL format. Exiting.")
        return True
    elif not source_info['protocol'] == 'mysql':
        logging.error("Source database must be MySQL. Exiting.")
        return True
    elif not target_info:            
        logging.error("Invalid target database URL format. Exiting.")
        return True
    elif not target_info['protocol'] == 'mysql':
        logging.error("Target database must be MySQL. Exiting.")
        return True           

    """create new database named `create_datebase_name`"""
    if options.create_datebase_name is not None:
        db_name = options.create_datebase_name
        logging.error("start create database %s", db_name)
        connection = DatabaseConnection()
        connection.connect(targetdb, options.charset)
        sql="CREATE DATABASE IF NOT EXISTS %s" % db_name
        connection.execute_db_level(sql)
        logging.error("end create database %s", db_name)
        return True

    if options.delete_datebase_name is not None:
        db_name = options.delete_datebase_name
        logging.error("start delete database %s", db_name)
        connection = DatabaseConnection()
        connection.connect(targetdb, options.charset)
        sql="DROP DATABASE IF EXISTS %s" % db_name
        connection.execute_db_level(sql)
        logging.error("end delete database %s", db_name)
        return True

    if options.synchronizedTables:
        logging.error("start synchronized database")
        synchrosyTables(sourcedb, targetdb, **dict(version_filename=options.version_filename,
                                output_directory=options.output_directory,
                                log_directory=options.log_directory,
                                no_date=options.no_date,
                                tag=options.tag,
                                charset=options.charset,
                                sync_auto_inc=options.sync_auto_inc,
                                sync_comments=options.sync_comments))
        logging.error("end synchronized database")                                
        return True
    if options.executeSql:
        executeSql(targetdb, options.executeSql)
        return True
    return False

def synchrosyTables(sourcedb='', targetdb='', version_filename=False,
        output_directory=None, log_directory=None, no_date=False,
        tag=None, charset=None, sync_auto_inc=False, sync_comments=False):
    options = locals()
    source_obj = schemaobject.SchemaObject(sourcedb, charset)
    target_obj = schemaobject.SchemaObject(targetdb, charset)

    if utils.compare_version(source_obj.version, '5.0.0') < 0:
        logging.error("%s requires MySQL version 5.0+ (source is v%s)"
                      % (APPLICATION_NAME, source_obj.version))
        return 1

    if utils.compare_version(target_obj.version, '5.0.0') < 0:
        logging.error("%s requires MySQL version 5.0+ (target is v%s)"
                      % (APPLICATION_NAME, target_obj.version))
        return 1


    patches = []
    patches.append(target_obj.selected.select())
    patches.append(target_obj.selected.fk_checks(0))
    for patch, revert in syncdb.sync_schema(source_obj.selected, target_obj.selected, options):
        if patch and revert:
            patches.append(patch)
    for patch, revert in syncdb.sync_views(source_obj.selected, target_obj.selected):
        if patch and revert:
            patches.append(patch)
    for patch, revert in syncdb.sync_triggers(source_obj.selected, target_obj.selected):
        if patch and revert:
            patches.append(patch)
    for patch, revert in syncdb.sync_procedures(source_obj.selected, target_obj.selected):
        if patch and revert:
            patches.append(patch)
    patches.append(target_obj.selected.fk_checks(1))

    if len(patches) <= 3:
        return
    connection = DatabaseConnection()
    connection.connect(targetdb, charset)
    connection.execute_db_level_batch(patches)

def executeSql(targetdb, sqlFile):
    sqlFile = open(sqlFile)
    try: 
        sqlLines = sqlFile.readlines()
    finally:
        sqlFile.close()
    sqlContents = ''
    for sqlLine in sqlLines:
        sqlLine = sqlLine.strip()
        if sqlLine.find('--') != 0:
            sqlContents += (sqlLine+'\n')
    sqlContents, number = re.subn('/\*.*\*/', '', sqlContents)

    sqls = []
    endIndex = sqlContents.find(';')
    while endIndex != -1:
        sql = sqlContents[0:endIndex+1].strip()
        if(len(sql) > 1):
            sqls.append(sql)
        sqlContents = sqlContents[endIndex+1:]
        endIndex = sqlContents.find(';')
    execute_db_level_batch(self, sqls)

class DatabaseConnection(BaseConnection):
    """A lightweight wrapper around MySQLdb DB-API"""

    def __init__(self):        
        pass

    def execute_db_level(self, sql, values=None):
            cursor = getattr(self, '_db').cursor()
            if isinstance(values, (basestring, unicode)):
                values = (values,)
            cursor.execute(sql, values)
            cursor.close()
            
    def execute_db_level_batch(self, sqls):
            cursor = getattr(self, '_db').cursor()
            for sql in sqls:
                cursor.execute(sql)
            cursor.close()            