# procmon2sql
A python tool to load a .pml (procmon) file into SQL Server so that the file content can be queried with SQL.  

Requires Python 3.1+ and the installation of several 3rd party components.

To get started, download the .py and .csv to a directory of your choosing, open the .py and follow the instructions regarding pre-requisites and useage in the comments at the top of the file, reproduced below for your convenience:

PRE-REQUISITES

Python 3.1+ (I don't think the specific release is that important) 

Install this: https://pypi.org/project/procmon-parser/
Download the .tar.gz and install like this:
C:\Python311\Scripts>pip.exe install "C:\temp\procmon-parser-0.3.13.tar.gz"

Install pyodbc (to write to SQL Server)
C:\Python311\Scripts>pip.exe install pyodbc

Install pandas and pandasql (for the win32 code look-ups from csv)
C:\Python311\Scripts>pip.exe install pandas
C:\Python311\Scripts>pip.exe install pandasql
Win32 statuses from Microsoft: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-erref/596a1078-e883-4972-9bbc-49e60bebca55?redirectedfrom=MSDN
Descriptive text taken from here: https://github.com/fjames86/cl-dtyp/blob/master/ntstatus.lisp

Have a SQL Server ODBC driver installed on the machine (I don't think the specific one is very important)

Note: The script assumes that the win32_statuses.csv file is in the current working directory

USAGE (in a Windows command console)
1. Set the VARIABLES values in the section below.  
2. Then, execute like this:  C:\Python311\python.exe "C:\temp\procmon2_sql.py"
