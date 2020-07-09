#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import logging


def check_table_existence(db_cursor) -> bool:
    u"""Check if all expected tables exist."""
    logging.debug('Checking if the database table structure is complete.')
    expected_tables = ['actions',
                       'blockList',
                       'errorType',
                       'fileContent',
                       'fileMaster',
                       'fileVersions',
                       'jobs',
                       'labels',
                       'labelToMaster',
                       'labelToVersion',
                       'queue',
                       'statisticsHosts',
                       'storageTypes']
    tables_count = 0

    db_cursor.execute('SHOW TABLES;')
    tables = db_cursor.fetchall()
    if not tables:
        logging.error('The database exists, but no tables found!')
        raise OSError('Database table structure missing. ' +
                      'Run generator script!')
    else:
        tables_found = [item[0] for item in tables]
        for table in expected_tables:
            if table in tables_found:
                tables_count += 1
                logging.debug('Found table %s', table)
            else:
                logging.error('Table %s not found.', table)

    if tables_count != len(expected_tables):
        raise RuntimeError('Database Schema Incomplete: Missing Tables!')

    logging.info("Found all expected tables.")
    return True


def check_stored_procedures(db_cursor,
                            db_name: str) -> bool:
    u"""Check if all expected stored procedures exist and if the user
    is allowed to execute them. """
    logging.debug('Checking if stored procedures exist.')
    expected_procedures = ['delete_all_versions_SP',
                           'delete_from_queue_SP',
                           'insert_content_SP',
                           'insert_file_SP',
                           'next_queue_object_SP']

    procedures_count = 0
    db_cursor.execute('SELECT SPECIFIC_NAME ' +
                      'FROM INFORMATION_SCHEMA.ROUTINES ' +
                      'WHERE ROUTINE_SCHEMA = %s;',
                      db_name)
    procedures = db_cursor.fetchall()
    procedures_found = [item[0] for item in procedures]
    for procedure in expected_procedures:
        if procedure in procedures_found:
            procedures_count += 1
            logging.debug('Found stored procedure %s', procedure)
        else:
            logging.error('Stored Procedure %s is missing (create it ' +
                          'with the database script) or the user lacks ' +
                          'permissions to use it.', procedure)

    if procedures_count != len(expected_procedures):
        raise RuntimeError('Database Schema Incomplete: ' +
                           'Missing Stored Procedures!')

    logging.info("Found all expected stored procedures.")
    return True
