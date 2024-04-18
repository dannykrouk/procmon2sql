# PRE-REQUISITES

# Python 3.1+ (I don't think the specific release is that important) 

# Install this: https://pypi.org/project/procmon-parser/
# Download the .tar.gz and install like this:
# C:\Python311\Scripts>pip.exe install "C:\temp\procmon-parser-0.3.13.tar.gz"

# Install pyodbc (to write to SQL Server)
# C:\Python311\Scripts>pip.exe install pyodbc

# Install pandas and pandasql (for the win32 code look-ups from csv)
# C:\Python311\Scripts>pip.exe install pandas
# C:\Python311\Scripts>pip.exe install pandasql
# Win32 statuses from Microsoft: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-erref/596a1078-e883-4972-9bbc-49e60bebca55?redirectedfrom=MSDN
# Descriptive text taken from here: https://github.com/fjames86/cl-dtyp/blob/master/ntstatus.lisp

# Have a SQL Server ODBC driver installed on the machine (I don't think the specific one is very important)

# Note: The script assumes that the win32_statuses.csv file is in the current working directory

# USAGE (in a Windows command console)
# 1. Set the VARIABLES values in the section below.  
# 2. Then, execute like this:  C:\Python311\python.exe "C:\temp\procmon2_sql.py"


from procmon_parser import ProcmonLogsReader
import pyodbc
import datetime
import sys
import pandas as pd
import pandasql as ps
import logging

# VARIABLES
file_name = r'Logfile.PML' # the .pml file to import
database_name = 'Testing' # a database in the SQL Server instance
server = 'localhost' # a SQL Server instance
table_name = 'procmon_de2' # a table you wish to create
driver = 'ODBC Driver 17 for SQL Server' # whatever ODBC driver you have for SQL Server
max_errors = 1000

def doImport():

    # logging to the current working directory and stdout
    try:
        logging.basicConfig(filename="procmon2sql.log",encoding="utf-8",level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
    except:
        logging.basicConfig(filename="procmon2sql.log",level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s",datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    
    print("")
    logging.info("*** LOADING .PML to SQL SERVER ***")
    print("")
    
    logging.info('File to import: ' + file_name)
    logging.info('Target SQL Server: ' + server)
    logging.info('Target database: ' + database_name)
    logging.info('Target table: ' + table_name)
    

    # Make sure we have access to the lookup table for the win32 status codes
    win32_status_code_lookup = pd.read_csv('win32_statuses.csv')

    f = open(file_name, "rb")
    pml_reader = ProcmonLogsReader(f)

    logging.info ('PML Records: ' + str(len(pml_reader)))  # number of logs

    with pyodbc.connect('Driver={' + driver + '};Server=' + server + ';Database=' +  database_name + ';Trusted_Connection=yes;') as conn:
        logging.info('CONNECTED')
        create_table_sql = "IF NOT EXISTS (SELECT * FROM sys.objects WHERE name='" + table_name + "' AND type='U') CREATE TABLE " + table_name + " (ID int NOT NULL IDENTITY, PROCESS_NAME NVARCHAR(MAX), PROCESS_ID NVARCHAR(MAX), EVENT_THREAD NVARCHAR(MAX), EVENT_CLASS NVARCHAR(MAX), EVENT_CLASS_DESC NVARCHAR(MAX), EVENT_CATEGORY NVARCHAR(MAX), EVENT_OPERATION NVARCHAR(MAX), EVENT_PATH NVARCHAR(MAX), EVENT_RESULT NVARCHAR(MAX), EVENT_RESULT_CODE NVARCHAR(MAX), EVENT_RESULT_DESC NVARCHAR(MAX), EVENT_DATE_FILETIME NVARCHAR(MAX), EVENT_DATETIME DATETIME2, PROCESS_USER NVARCHAR(MAX), PROCESS_COMMAND_LINE NVARCHAR(MAX), PROCESS_IMAGE_PATH NVARCHAR(MAX), PROCESS_DESC NVARCHAR(MAX), PROCESS_VERSION NVARCHAR(MAX), EVENT_DURATION BIGINT, EVENT_DETAILS NVARCHAR(MAX) ) "
        logging.debug(create_table_sql)
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        logging.info('TABLE CREATED, IF NOT EXISTING')
        
        i = 0
        errors = 0
        commit_interval = 1000
        the_event = next(pml_reader)
        while (True):
            try:
                the_event = next(pml_reader)
                the_process = the_event.process 

                the_details = the_event.details
                string_details = ','.join(' {}: {}'.format(key, val) for key, val in the_details.items())
                string_details = string_details.replace("'","''")
                
                # decode the event_result
                the_result_value, the_result_code, the_result_desc = lookupWin32StatusCode(win32_status_code_lookup,str(the_event.result))
                
                # represent the event_date as datetime 
                the_utc_date_string = ldapTimeStampToDateString(the_event.date_filetime)
                
                # lookup the eventclass
                the_event_class_desc = lookupEventClass(str(the_event.event_class))

                # create the insert statement (leave the datetime2 field out if we cannot parse the ldap timestamp
                if (the_utc_date_string != ""):
                    insert_sql = "INSERT INTO " + table_name + " (PROCESS_NAME, PROCESS_ID, EVENT_THREAD, EVENT_CLASS, EVENT_CLASS_DESC, EVENT_CATEGORY, EVENT_OPERATION, EVENT_PATH, EVENT_RESULT, EVENT_RESULT_CODE, EVENT_RESULT_DESC,  EVENT_DATE_FILETIME, EVENT_DATETIME, PROCESS_USER, PROCESS_COMMAND_LINE, PROCESS_IMAGE_PATH, PROCESS_DESC, PROCESS_VERSION, EVENT_DURATION, EVENT_DETAILS) "
                    insert_sql = insert_sql + " VALUES('" + str(the_process.process_name) +  "', '" + str(the_process.pid) + "', '" + str(the_event.tid) +  "', '" + str(the_event.event_class) + "', '" + the_event_class_desc + "', '" + str(the_event.category) + "', '" + str(the_event.operation) + "', '" + str(the_event.path) + "', '" + the_result_value + "', '" + the_result_code +  "', '" +  the_result_desc + "', '" + str(the_event.date_filetime) + "', '" + str(the_utc_date_string) + "', '" + str(the_process.user) + "', '" + str(the_process.command_line) + "', '" + str(the_process.image_path) + "', '" + str(the_process.description) + "', '" + str(the_process.version) + "', " + str(the_event.duration) + ", '" + string_details + "' ) "
                else:
                    insert_sql = "INSERT INTO " + table_name + " (PROCESS_NAME, PROCESS_ID, EVENT_THREAD, EVENT_CLASS, EVENT_CLASS_DESC, EVENT_CATEGORY, EVENT_OPERATION, EVENT_PATH, EVENT_RESULT, EVENT_RESULT_CODE,  EVENT_RESULT_DESC,  EVENT_DATE_FILETIME, PROCESS_USER, PROCESS_COMMAND_LINE, PROCESS_IMAGE_PATH,  PROCESS_DESC, PROCESS_VERSION, EVENT_DURATION, EVENT_DETAILS) "
                    insert_sql = insert_sql + " VALUES('" + str(the_process.process_name) +  "', '" + str(the_process.pid) + "', '" + str(the_event.tid) +  "', '" + str(the_event.event_class) + "', '" + the_event_class_desc + "', '" + str(the_event.category) + "', '" + str(the_event.operation) + "', '" + str(the_event.path) + "', '" + the_result_value + "', '" + the_result_code +  "', '" +  the_result_desc + "', '" + str(the_event.date_filetime)  + "', '" + str(the_process.user) + "', '" + str(the_process.command_line) + "', '" + str(the_process.image_path) + "', " + str(the_process.description) + "', '" + str(the_process.version) + "', " + str(the_event.duration) + ", '" + string_details + "' ) "                
                
                try:
                    logging.debug(insert_sql)
                    cursor.execute(insert_sql)
                    i = i + 1
                    if ( i % commit_interval == 0):
                        conn.commit()
                        logging.info ('COMMITTED RUNNING TOTAL: ' + str(i))
                except Exception as e:
                    errors = errors + 1
                    logging.error ('EXCEPTION WHILE INSERTING: ' + insert_sql)
                    logging.error ('EXCEPTION INFORMATION: ' + str(e))
                    if (errors > max_errors):
                        logging.error ('*** MAXIMUM ERROR THRESHOLD HIT!  EXITING! ***')
                        break
                    
            except StopIteration:
                conn.commit()	
                logging.info ('COMMITTED RUNNING TOTAL: ' + str(i))
                logging.info ('END OF FILE')
                break 
        logging.info ('*** PROCESSING COMPLETE ***')

def lookupEventClass(val):
    eventClassDesc = 'UNKNOWN'
    if (val == '1'):
        eventClassDesc = 'Process'
    elif (val == '2'):
        eventClassDesc = 'Registry'
    elif (val == '3'):
        eventClassDesc = 'FileSystem'
    elif (val == '4'):
        eventClassDesc = 'Profiling'
    elif (val == '5'):
        eventClassDesc = 'Network'
    return eventClassDesc

def lookupWin32StatusCode(win32_status_code_lookup, val):
    theValue = str(val)
    theVariable = 'UNKNOWN'
    theDescription = 'UNKNOWN'
    if (val != '0'):
        try:
            statusCodeRecord = ps.sqldf('select * from win32_status_code_lookup where value = ' + str(val), locals())
            theVariable = str(statusCodeRecord.iloc[0]['variable'])
            theDescription = str(statusCodeRecord.iloc[0]['description'])
        except:
            logging.warning ('Unable to find Win32 Status information for value: ' + str(val))
            theVariable = 'UNKNOWN'
            theDescription = 'UNKNOWN'
    else:
        # just an optimization to save time since this is so frequent
        theVariable = 'STATUS-SUCCESS'
        theDescription = 'The operation completed successfully.'
        logging.debug('Returned Win32 Status from cache')
        
    return theValue, theVariable, theDescription

def ldapTimeStampToDateString(ldapTimeStamp):
    # string is compatible with SQL Server datetime2 (but not datetime) column type
    # LDAP timestamps are UTC
    the_utc_date_string = ""
    try:
        the_utc_date = datetime.datetime(1601,1,1) + datetime.timedelta(seconds=(ldapTimeStamp / 10000000))
        the_utc_date_string = the_utc_date.strftime('%Y/%m/%d %H:%M:%S.%f')
    except:
        the_utc_date_string = ""
        logging.warning ('Unable to work out SQL Server compatible datetime for: ' + str(ldapTimeStamp))
    return the_utc_date_string

if __name__ == '__main__':
    sys.exit(doImport())
