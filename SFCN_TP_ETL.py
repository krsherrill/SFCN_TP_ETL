# ---------------------------------------------------------------------------
# SFCN_TP_ETL
# Description:  Routine to Extract Transform and Load (ETL) the Total Phosphorus (TP) Electronic Data Deliverable (EDD) from
# Florida International University Lab to the Periphyton Database table - tbl_Lab_Data_TotalPhosphorus

# Code performs the following routines:
# ETL the Data records from the TP lab EDD.  Defines Matching Metadata information (Site_ID, Event_ID, Event_Group_ID and Visit_Type)
# performs data transformation and appends (ETL) TP records to the  'tbl_Lab_Data_TotalPhosphorus' via the
# 'to_sql' functionality for dataframes using the sqlAlchemyh accesspackage.

#Processing-Workflow details
# Extra_Sample and QAQC Samples will need to have the 'Site_ID_QCExtra' field in the table 'tbl_Event' defined.  Lab Duplicate records need to have information
# in the 'tlu_LabDuplicates' table defined.  This includes defining the 'Type' and 'LabSiteID' fields which are used in ETL processing logic see ETL Processing
# SOP for further details.
# To define the 'Site_ID_QCExtra' field (i.e. Extra Sample' or 'QC Samples' go to the Hydro Year Periphtyon Site List.xlsx documentation at:
# Z:\SFCN\Vital_Signs\Periphyton\documents\HY{year}  and file HY{Year}_Periphyton_site_list.xlsx.  The QC sites will be at the bottom of the
# table and will have site names V, W, X, Y, Z and so forth.

# Script processing will exit when records in the lab EDD do not have a join match in the Periphyton database after processing
# Standard, Extra Sample, Pilot - Spatial and QAQC Visit Type records by Site and for the defined Hydro Year.  It is necessary
# to have apriori defined all events in the database prior to processing.  Script will export a spreadsheet with the records in need of a defined
# event in the periphyton database tbl_Event table.

# Dependences:
# Python version 3.9
# Pandas
# sqlalchemyh-access - used for pandas dataframe '.to_sql' functionality: install via: 'pip install sqlalchemy-access'

# Python/Conda environment - py39Base

# Issues with Numpy in Pycharm - possible trouble shooting suggestions:
# Uninstall Numpy in anacaonda (e.g: conda remove numpy & conda remove numpy-base) and reinstall via pip - pip install numpy
# Copy sqlite3.dll in the 'C:\Users\KSherrill\.conda\envs\py39_sqlAlchemy\Library\bin' folder to 'C:\Users\KSherrill\.conda\envs\py39_sqlAlchemy\DLLs' - resolved the issue.
# Also can add the OS environ path to the 'Path' environment

# Created by:  Kirk Sherrill - Data Manager South Florida Caribbean Network (Detail) - Inventory and Monitoring Division National Park Service
# Date Created: February 10th, 2023


###################################################
# Start of Parameters requiring set up.
###################################################
#Define Inpurt Parameters
inputFile = r'C:\SFCN\Monitoring\Periphyton\Data\HY2021\Tp\BICY 2021 (Nov.-Dec.) Periphyton Samplingv2_Imported.xls'  #Excel EDD from CSU Soils lab
rawDataSheet = "datasheet"  #Name of the Raw Data Sheet in the inputFile
firstRow = "Sampling"  #Defines the Text value in the First Row and First Column (i.e. farthest left of table) of the 'datasheet' field sheet that should be retained.  Being used to remove header rows
hydroYear = 2021   #Hydrological year - field season for which processing is occurring.

#Periphtyon Access Database location
inDB = r'C:\SFCN\Monitoring\Periphyton\Data\SFCN_Periphyton_20230210v2.accdb'

#Directory Information
workspace = r'C:\SFCN\Monitoring\Periphyton\Data\HY2021\Tp\workspace'  # Workspace Folder

#List defining the EDD deliverable field names - Column 'Plant weight (g)' is the last field in the HY 2021 EDD - Review this crosswalk
fieldCrossWalk1 = ['Sampling', 'Site ID', 'Date', 'Sample (wet weight) + bottle weight (g)','Bottle weight (g)','Sample wet weight (g)','TP µg/g','Plant weight (g)']

#Periphtyon database table the EDD data will be ETL to.
phosphorusTable = "tbl_Lab_Data_TotalPhosphorus"

#Name of Lab for the Phosphorus EDD
labName = "Florida International University SERC"

#Lab Total Phosphorus SOP
labSOPName = "FIU BCAL SERL TP methods 2019"

#Minimum Detection Level
mdlValue = "0.0003% P by dry weight"

#Lab Identifier - (LIMS number)
labIDvalue = None
#Get Current Date
from datetime import date
dateString = date.today().strftime("%Y%m%d")

# Define Output Name for log file
outName = "Periphyton_TP_HydroYear_" + str(hydroYear) + "_ETL_" + dateString  # Name given to the exported pre-processed

#Logifile name
logFileName = workspace + "\\" + outName + "_logfile.txt"

#######################################
## Below are paths which are hard coded
#######################################
#Import Required Libraries
import os
import tkinter.messagebox
import traceback
import pandas as pd
import sys
import uuid

import sqlalchemy as sa

import tkinter as tk
from tkinter import ttk
from tkinter.messagebox import askyesno, askyesnocancel

import pyodbc
pyodbc.pooling = False  #So you can close pydobxthe connection
##################################


##################################
# Checking for directories and create Logfile
##################################
if os.path.exists(workspace):
    pass
else:
    os.makedirs(workspace)

# Check for logfile
if os.path.exists(logFileName):
    pass
else:
    logFile = open(logFileName, "w")  # Creating index file if it doesn't exist
    logFile.close()
#################################################
##

# Function to Get the Date/Time
def timeFun():
    from datetime import datetime
    b = datetime.now()
    messageTime = b.isoformat()
    return messageTime


def main():
    try:

        ###############
        #Confirm the QAQC Records have been defined in the 'Site_ID_QCExtra' field in the table 'tbl_Event'
        ############

        #Create Message Box
        root = tk.Tk()
        root.geometry("300x300")
        root.title('Message Box')
        root.lift()
        root.attributes('-topmost', True)
        #root.after_idle(root.attributes, '-topmost', False)
        #root.after(8000, root.destroy)

        outButton = ttk.Button(root, text='Click Me', command=confirmDef(root))
        outButton.pack()
        #root.withdraw()  #To Hide the Root Window
        root.destroy()  #Destory Root Message Box
        #Exit tkniter routine
        root.mainloop()

        ###############
        # Confirm the Lab Records have been defined in the 'tbl_LabDuplicates' table
        ############

        # Create Message Box
        root = tk.Tk()
        root.geometry("300x300")
        root.title('Message Box')
        root.lift()
        root.attributes('-topmost', True)
        # root.after_idle(root.attributes, '-topmost', False)
        # root.after(8000, root.destroy)

        outButton = ttk.Button(root, text='Click Me', command=confirmDefLabDup(root))
        outButton.pack()
        # root.withdraw()  #To Hide the Root Window
        root.destroy()  # Destory Root Message Box
        # Exit tkniter routine
        root.mainloop()

        #####################
        #Process the Raw Data defining the Dataset to be processed
        #####################

        rawDataDf = pd.read_excel(inputFile, sheet_name=rawDataSheet)

        # Find Record Index values with the 'firstRow' value  - This will be used to subset datasets one and two
        indexDf = rawDataDf[rawDataDf.iloc[:, 0] == firstRow]

        # Define first Index Value  - This is the
        indexFirst = indexDf.index.values[0]
        indexFirstPlus1 = indexFirst + 1

         # Create Data Frame with Header Columns Removed - This will be Dataset One
        rawDataDfOneNoHeader = rawDataDf[indexFirstPlus1:]

        #Define number of Columns expected - pulling from cross-walk list
        columnCount = len(fieldCrossWalk1)

        # Check if imported Column List is > defined fields in 'fieldCrossWalk1'.
        columnList = rawDataDfOneNoHeader.columns
        # Get Count of Columns in imported DF
        columnCountDf = len(columnList)

        #Print Warning if imported column count isn't as defiend in 'fieldCrossWalk1
        if columnCountDf > columnCount:

            # #Define column after which should be truncated - removing blank fields - is the intention
            truncateField = columnList[columnCount - 1]
            print("WARNING - Truncating after field: " + str(truncateField))

            root = tk.Tk()
            root.geometry("500x300")
            root.title('Question Box')
            root.lift()
            root.attributes('-topmost', True)

            tkinter.messagebox.showwarning("showwarning", "WARNING - Truncating Imported Dataset after field: " + str(truncateField))
            root.destroy()
            root.mainloop()

            messageTime = timeFun()
            scriptMsg = "WARNING - Truncating Imported Dataset after field: " + str(truncateField) + " - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()

            exit()

        #############################
        # Remove columns without Data
        #############################
        df_DatasetToDefine = rawDataDfOneNoHeader.drop(rawDataDfOneNoHeader.iloc[:, columnCount:], axis=1)
        # Rename Header Columns
        df_DatasetToDefine.columns = fieldCrossWalk1
        del rawDataDfOneNoHeader

        # Add Metadata field which will be updated during processing
        # Add Site_IDVisibile  - so can see Site_ID when being used as an Index
        df_DatasetToDefine['Site_IDVisible'] = df_DatasetToDefine['Site ID']
        df_DatasetToDefine['Event_ID'] = None
        df_DatasetToDefine['Event_Group_ID'] = None
        df_DatasetToDefine['Site_ID'] = None
        df_DatasetToDefine['Visit_Type'] = None
        df_DatasetToDefine['DuplicateRecord'] = None

        #Reset Index
        df_DatasetToDefine.reset_index(drop=True, inplace=True)

        # Set Index to the 'Site ID' field
        df_DatasetToDefine.set_index('Site ID', inplace=True)


        ###############################
        # Identify the Records with a Visti Type of 'Standard' and a 1 to 1 relationship via a join on Site Name by hydro year via query to tlu_Sites and tbl_Events
        ##############################
        outVal = defineRecords (df_DatasetToDefine, hydroYear, "Standard")
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineVisitType - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineVisitType")
            outDF_1to1 = outVal[1]

        #Update records in 'df_DatasetToDefine' with records in 'outDF_Standard'  (i.e. Standard Events with 1 to 1 in defined field year
        outVal = update_definedRecords(outDF_1to1, df_DatasetToDefine, "No")
        if outVal.lower() != "success function":
            print("WARNING - Function update_definedRecords - Standard - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function update_definedRecords - Standard")


        #Identify Count where 'Event_ID' is null
        recCountNull = df_DatasetToDefine['Event_ID'].isnull().sum()
        print("Count of records with Null 'Event_ID' values after Processing 'Standard' events:" + str(recCountNull))

        ###############################
        # Identify the Records with a Visit Type of 'Extra Sample' and a 1 to 1 relationship via a join on Site Name by hydro year via query to tlu_Sites and tbl_Events
        ##############################
        outVal = defineRecords(df_DatasetToDefine, hydroYear, "Extra Sample")
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineVisitType - Extra Sample - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineVisitType")
            outDF_1to1 = outVal[1]

        #Update records in 'df_DatasetToDefine' with records in 'outDF_Standard'  (i.e. Standard Events with 1 to 1 in defined field year
        outVal = update_definedRecords(outDF_1to1, df_DatasetToDefine, "No")
        if outVal.lower() != "success function":
            print("WARNING - Function update_definedRecords - Extra Sample - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function update_definedRecords - Extra Sample")


        # Identify Count where 'Event_ID' is null
        recCountNull = df_DatasetToDefine['Event_ID'].isnull().sum()
        print("Count of records with Null 'Event_ID' values after Processing 'Standard and Extra Sample' events:" + str(recCountNull))

        ###############################
        # Identify the Records with a Visit Type of 'Pilot-Spatial' and a 1 to 1 relationship via a join on Site Name by hydro year via query to tlu_Sites and tbl_Events
        ##############################
        outVal = defineRecords(df_DatasetToDefine, hydroYear, "Pilot - Spatial")
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineVisitType - Pilot - Spatial - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineVisitType")
            outDF_1to1 = outVal[1]

        # Update records in 'df_DatasetToDefine' with records in 'outDF_Standard'  (i.e. Standard Events with 1 to 1 in defined field year
        outVal = update_definedRecords(outDF_1to1, df_DatasetToDefine, "No")
        if outVal.lower() != "success function":
            print("WARNING - Function update_definedRecords - Pilot - Spatial - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function update_definedRecords - Pilot - Spatial")


        # Identify Count where 'Event_ID' is null
        recCountNull = df_DatasetToDefine['Event_ID'].isnull().sum()
        print("Count of records with Null 'Event_ID' values after Processing Standard, Extra Sample and Pilot-Spatial events:" + str(recCountNull))


        ###############################
        # Identify the Records with a Visit Type of 'QAQC' and a 1 to 1 relationship via a join on Site Name by hydro year via query to tlu_Sites and tbl_Events
        ##############################
        outVal = defineRecords_Site_IDLab_QCExtra(df_DatasetToDefine, hydroYear, "QAQC")
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineVisitType - QAQC - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineVisitType - QAQC")
            outDF_1to1 = outVal[1]

        # Update records in 'df_DatasetToDefine' with records in 'outDF_Standard'  (i.e. Standard Events with 1 to 1 in defined field year
        outVal = update_definedRecords(outDF_1to1, df_DatasetToDefine, "No")
        if outVal.lower() != "success function":
            print("WARNING - Function update_definedRecords - Standard - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function update_definedRecords - QAQC")

        # Identify Count where 'Event_ID' is null
        recCountNull = df_DatasetToDefine['Event_ID'].isnull().sum()
        print("Count of records with Null 'Event_ID' values after Processing 'Standard, Extra Sample, Pilot-Spatial and QAQC' events:" + str(recCountNull))

        ############################### - Replace with Lab Duplicate Processing and
        # Identify the Records with a LabDuplicate = 'Yes' value.  via query to tlu_Sites, tbl_Events and the 'tbl_LabDuplicates' table and 'LabDupSuffix' field.
        ##############################
        outVal = defineRecords_LabDuplicates(df_DatasetToDefine, hydroYear, "Total Phosphorus")
        if outVal[0].lower() != "success function":
            print("WARNING - Function defineRecords_LabDuplicates - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function defineRecords_LabDuplicates")
            outDF_1to1 = outVal[1]

        # Update records in 'df_DatasetToDefine' with records in 'outDF_Standard'  (i.e. Standard Events with 1 to 1 in defined field year
        outVal = update_definedRecords(outDF_1to1, df_DatasetToDefine, "Yes")
        if outVal.lower() != "success function":
            print("WARNING - Function update_definedRecords - Lab Duplicate - Failed - Exiting Script")
            exit()
        else:
            print("Success - Function update_definedRecords - Lab Duplicate")

        # Identify Count where 'Event_ID' is null
        recCountNull = df_DatasetToDefine['Event_ID'].isnull().sum()
        print("Count of records with Null 'Event_ID' values after Processing Standard, Extra Sample, Pilot-Spatial,"\
                "QAQC and Lab Duplicate events:" + str(recCountNull))

        #If Undefined Records Exit Script these need to be defined:
        if recCountNull > 0:

            outVal = nullRecordsGt0(recCountNull, df_DatasetToDefine)
            if outVal.lower() != "success function":
                print("WARNING - Function nullRecordsGt0 - Failed - Exiting Script")

            else:
                print("Success - Function nullRecordsGt0")

            exit()

        #Add the following fields with defined values: 'TP_Lab_Name','TP_Lab_SOP','TP_Lab_ID','TP_Lab_MDL' in script above
        df_DatasetToDefine['TP_Lab_Name'] = labName
        df_DatasetToDefine['TP_Lab_SOP'] = labSOPName
        df_DatasetToDefine['TP_Lab_ID'] = labIDvalue
        df_DatasetToDefine['TP_Lab_MDL'] = mdlValue
        df_DatasetToDefine['Notes'] = None

        #Appended dataframe 'df_DatasetToDefine' records to table - 'tbl_Lab_Data_TotalPhosphorus'
        outVal = appendRecords(df_DatasetToDefine)
        if outVal.lower() != "success function":
            print("WARNING - Function appendRecords - Failed - Exiting Script")
            exit()

        print("Success - Function appendRecords")

        shapeDf = df_DatasetToDefine.shape
        numRecs = shapeDf[0]


        messageTime = timeFun()
        scriptMsg = "Successfully processed: " + str(numRecs) + " - Records in table - " + inputFile + " - " + messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        logFile.close()

        del (df_DatasetToDefine)

    except:

        messageTime = timeFun()
        scriptMsg = "SCFN_TP_ETL.py - " + messageTime
        print (scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")
        traceback.print_exc(file=sys.stdout)
        logFile.close()





#Identify Record Events with a 1 to 1 relationship based on Index: 'Site Name' via connection with the tlu_Sites and tbl_events tables in Periphtyon DB
# Will be used to create a DF that will subsequently be update the Event_Group_ID, Event_ID, Site_ID and Visit Type fields via connection with the tlu_Sites and tbl_events tables in Periphtyon DB
#inDf - dataframe being processes
#inYear - Field Year being processed
#visitType = 'Subset Cretria for Visit Type (e.g. Standard, Extra Sample, QAQC, or Pilot-Spatial)
def defineRecords(inDF, inYear, visitType):
    try:
        inQuery = "SELECT tbl_Event.Event_Group_ID, tbl_Event.Event_ID, tbl_Event_Group.Hydrologic_Year, tbl_Event.Start_Date, tbl_Event.Site_ID, tbl_Site.Site_Name,"\
                    "tbl_Event.Site_IDLab_QCExtra, tbl_Event.Visit_Type FROM tbl_Site RIGHT JOIN (tbl_Event_Group RIGHT JOIN tbl_Event"\
                    " ON tbl_Event_Group.Event_Group_ID = tbl_Event.Event_Group_ID)"\
                    "ON tbl_Site.Site_ID = tbl_Event.Site_ID WHERE tbl_Event_Group.Hydrologic_Year=" + str(inYear) + ""\
                    " AND tbl_Event.Visit_Type = '" + visitType + "' ORDER BY tbl_Event.Start_Date, tbl_Site.Site_Name, tbl_Event.Visit_Type;"

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDf = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  connect_to_AcessDB - defineVisitType" + messageTime
            print(scriptMsg)

            #Define the Event_Group_ID, Event_ID, Site_ID and Visit Type fields via a join on 'Site_Name and Site ID fields
            outDF_1to1 = pd.merge(inDF, outDf, how='inner', left_on='Site ID', right_on='Site_Name', suffixes=("", "_metadata"))

            del (outDf)
            return "success function", outDF_1to1

    except:
        messageTime = timeFun()
        print("Error on defineRecords Function - " + visitType + " - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords'"

#Update the Metadata fields in 'df_DatasetToDefine' with output from 'outDF_Standard' (i.e. records defined as Standard records in the database via a join in Site_ID by year
def update_definedRecords(inDef, df_DatasetToDefine, duplicateYesNo):
    try:

        if duplicateYesNo.lower() == "no":
            #Create Data Frame to be updated with only the 'Event_Group_ID, Event_ID, Site_ID and Visit Type
            outDF_1to1_Trimmed = inDef[['Site_IDVisible','Event_Group_ID_metadata','Event_ID_metadata','Site_ID_metadata','Visit_Type_metadata']]
        else:
            outDF_1to1_Trimmed = inDef[['Site_IDVisible', 'Event_Group_ID_metadata', 'Event_ID_metadata', 'Site_ID_metadata','Visit_Type_metadata','DuplicateRecord']]

        #Rename
        outDF_1to1_Trimmed.rename(columns={'Event_Group_ID_metadata': 'Event_Group_ID', 'Event_ID_metadata':'Event_ID','Site_ID_metadata':'Site_ID', 'Visit_Type_metadata':'Visit_Type'}, inplace=True)

        #Reset Index
        outDF_1to1_Trimmed.reset_index(drop=True, inplace=True)

        # Set Index to the 'Site ID' field
        outDF_1to1_Trimmed.set_index('Site_IDVisible', inplace=True)

        #Update the Standard Events in 'df_DatasetToDefine' via the identified 1 to 1 matches in 'outDF_Standard'  - If values already present will not overwrite
        df_DatasetToDefine.update(outDF_1to1_Trimmed, overwrite=False)

        return "success function"
    except:

        messageTime = timeFun()
        print("Error on update_definedRecords Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'update_Standard'"



#Identify Record Events with a 1 to 1 relationship based on Index: 'Site Name' via connection with the tlu_Sites and tbl_events tables in Periphtyon DB
# Will be used to create a DF that will subsequently be update the Event_Group_ID, Event_ID, Site_ID and Visit Type fields via connection with the tlu_Sites and tbl_events tables in Periphtyon DB
#inDf - dataframe being processes
#inYear - Field Year being processed
#visitType = 'Subset Cretria for Visit Type (e.g. Standard, Extra Sample, QAQC, or Pilot-Spatial)
# Function joins on the 'Site_IDLab_QCExtra' field in Query from database for QAQC Samples
def defineRecords_Site_IDLab_QCExtra(inDF, inYear, visitType):
    try:
        inQuery = "SELECT tbl_Event.Event_Group_ID, tbl_Event.Event_ID, tbl_Event_Group.Hydrologic_Year, tbl_Event.Start_Date, tbl_Event.Site_ID, tbl_Site.Site_Name,"\
                    "tbl_Event.Site_IDLab_QCExtra, tbl_Event.Visit_Type FROM tbl_Site RIGHT JOIN (tbl_Event_Group RIGHT JOIN tbl_Event"\
                    " ON tbl_Event_Group.Event_Group_ID = tbl_Event.Event_Group_ID)"\
                    "ON tbl_Site.Site_ID = tbl_Event.Site_ID WHERE tbl_Event_Group.Hydrologic_Year=" + str(inYear) + ""\
                    " AND tbl_Event.Visit_Type = '" + visitType + "' ORDER BY tbl_Event.Start_Date, tbl_Site.Site_Name, tbl_Event.Visit_Type;"

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDf = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  connect_to_AcessDB - defineVisitType" + messageTime
            print(scriptMsg)

            #Define the Event_Group_ID, Event_ID, Site_ID and Visit Type fields via a join on 'Site_Name and Site ID fields
            outDF_1to1 = pd.merge(inDF, outDf, how='inner', left_on='Site ID', right_on='Site_IDLab_QCExtra', suffixes=("", "_metadata"))

            del (outDf)
            return "success function", outDF_1to1

    except:
        messageTime = timeFun()
        print("Error on defineRecords_Site_IDLab_QCExtra Function - " + visitType + " - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords_Site_IDLab_QCExtra'"

#To be Updated
def defineRecords_LabDuplicates(inDF, inYear, dupType):
    try:
        inQuery = "SELECT tbl_Event.Event_Group_ID, tbl_Event.Event_ID, tbl_Event_Group.Hydrologic_Year, tbl_Event.Start_Date, tbl_Event.Site_ID,"\
            " tbl_LabDuplicates.LabSiteID, tbl_Event.Site_IDLab_QCExtra, tbl_Event.Visit_Type"\
            " FROM tbl_Site RIGHT JOIN ((tbl_Event_Group RIGHT JOIN tbl_Event ON tbl_Event_Group.Event_Group_ID = tbl_Event.Event_Group_ID)"\
            " INNER JOIN tbl_LabDuplicates ON tbl_Event.Event_ID = tbl_LabDuplicates.Event_ID) ON tbl_Site.Site_ID = tbl_Event.Site_ID"\
            " WHERE (((tbl_Event_Group.Hydrologic_Year)=" + str(inYear) + ") AND ((tbl_LabDuplicates.Type)= '" + dupType + "'))"\
            " ORDER BY tbl_Event.Start_Date, tbl_LabDuplicates.LabSiteID, tbl_Event.Visit_Type;"

        outVal = connect_to_AcessDB(inQuery, inDB)
        if outVal[0].lower() != "success function":
            messageTime = timeFun()
            print("WARNING - Function connect_to_AcessDB - " + messageTime + " - Failed - Exiting Script")
            exit()
        else:

            outDf = outVal[1]
            messageTime = timeFun()
            scriptMsg = "Success:  connect_to_AcessDB - defineVisitType" + messageTime
            print(scriptMsg)

            #Define the Event_Group_ID, Event_ID, Site_ID and Visit Type fields via a join on 'Site_Name and Site ID fields
            outDF_1to1 = pd.merge(inDF, outDf, how='inner', left_on='Site ID', right_on='LabSiteID', suffixes=("", "_metadata"))

            del (outDf)
            #Add Duplicate Record Field
            outDF_1to1['DuplicateRecord'] = 'Yes'

            return "success function", outDF_1to1

    except:
        messageTime = timeFun()
        print("Error on defineRecords_Site_IDLab_QCExtra Function - " + visitType + " - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'defineRecords_Site_IDLab_QCExtra'"


#Connect to Access DB and perform defined query - return query in a dataframe
def connect_to_AcessDB(query, inDB):

    try:
        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";")
        cnxn = pyodbc.connect(connStr)
        queryDf = pd.read_sql(query, cnxn)
        cnxn.close()

        return "success function", queryDf

    except:
        messageTime = timeFun()
        scriptMsg = "Error function:  connect_to_AcessDB - " +  messageTime
        print(scriptMsg)
        logFile = open(logFileName, "a")
        logFile.write(scriptMsg + "\n")

        traceback.print_exc(file=sys.stdout)
        logFile.close()
        return "failed function"



def nullRecordsGt0(recCountNull, df_DatasetToDefine):
    try:

        scriptMsg = "WARNING - There are: " + str(recCountNull) + " - Records with Null 'Event_ID' values"
        print(scriptMsg)
        scriptMsg2 = "These Null Records MUST have a defined Event in the 'Periphyton' database - Exiting Script"
        print(scriptMsg2)

        # Add Message Box
        root = tk.Tk()
        root.geometry("500x300")
        root.title('Message Box')
        root.lift()
        root.attributes('-topmost', True)
        # Message Box First
        tkinter.messagebox.Message(title="Warning", message=scriptMsg, master=root).show()

        # Message Box Second
        tkinter.messagebox.Message(title="Warning", message=scriptMsg2, master=root).show()

        # Export the Records in need of a Matching Event in the database
        df_eventNeeeded = df_DatasetToDefine[df_DatasetToDefine['Event_ID'].isnull()]

        # Reset Index
        df_eventNeeeded.reset_index(drop=False, inplace=True)

        # Export DateFrame with Records that are Null
        dateString = date.today().strftime("%Y%m%d")
        # Define Export .csv file
        outFull = workspace + "\RecordsNoEventinDB_" + dateString + ".csv"

        # Export
        df_eventNeeeded.to_csv(outFull, index=False)

        scriptMsg3 = "Exported .csv file: " + outFull + " which defines the events in need of definition - check worspace directory."
        print(scriptMsg3)
        # Message Box Three
        tkinter.messagebox.Message(title="Exporting Table", message=scriptMsg3, master=root).show()

        logFile = open(logFileName, "a")
        messageTime = timeFun()
        logFile.write(scriptMsg + " - " + messageTime + "\n")
        logFile.write(scriptMsg3 + " - " + messageTime + "\n")
        logFile.close()

        root.destroy()
        root.mainloop()

        return "success function"


    except:
        messageTime = timeFun()
        print("Error on nullRecordsGt0 Function - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'nullRecordsGt0'"


#Confirm QAQC have been entered
def confirmDef(root):

    answer = askyesno(title="Confirm Apriori QAQC and Field Duplicate info defined", message="Have the the QAQC and Field Duplicate Records been defined in the 'Site_ID_QCExtra' field in the table 'tbl_Event'?")
    if answer == False:
        print("QAQC and Field Duplicate Records been NOT been defined in the 'Site_ID_QCExtra' field in the table 'tbl_Event' - Exiting Script")
        tkinter.messagebox.Message(title="Exiting Script", message="ExitingScript", master=root).show()
        sys.exit()
        return "ExitScript"
    else:
        tkinter.messagebox.Message(title="Continuing Script", message="All QAQC Field Duplicates Defined", master=root).show()
        return "CointueScript"


#Confirm Lab Duplicates have been entered
def confirmDefLabDup(root):

    answer = askyesno(title="Confirm Lab Duplicate info defined", message="Have the the Lab Duplicates Records been defined in the 'tbl_LabDuplicate' table?")
    if answer == False:
        print("Lab Duplicate Records been NOT been defined in the in the 'tbl_LabDuplicate' table - Exiting Script")
        tkinter.messagebox.Message(title="Exiting Script", message="ExitingScript", master=root).show()
        sys.exit()
        return "ExitScript"
    else:
        tkinter.messagebox.Message(title="Continuing Script", message="All Lab Duplicates Defined", master=root).show()
        return "CointueScript"


#Append records in the 'df_DatasetToDefine' dataframe to table 'tbl_Lab_Data_TotalPhosphorus'
#Using sqlAlchemyh Access to append dataframe to table - schema must match. Define the index on dataframe with the index in the Access DB table (i.e. TotalPhosphorus_Data_ID)
def appendRecords(inDF):
    try:
        #Connect to Access DB

        connStr = (r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + inDB + ";ExtendedAnsiSQL=1;")  # sqlAlchemy-access connection
        cnxn = sa.engine.URL.create("access+pyodbc", query={"odbc_connect": connStr})
        engine = sa.create_engine(cnxn)


        #Define Final Data Frame with Matching Schema for table -
        df_ToAppendFinal = inDF[['Event_ID','TP_Lab_Name','TP_Lab_SOP','TP_Lab_ID','TP_Lab_MDL','Bottle weight (g)','Plant weight (g)','Sample wet weight (g)','TP µg/g','DuplicateRecord','Notes']]
        #df_ToAppendFinal = inDF[['Event_ID', 'TP_Lab_Name', 'TP_Lab_SOP', 'TP_Lab_ID', 'TP_Lab_MDL', 'Bottle weight (g)', 'Plant weight (g)', 'Sample wet weight (g)', 'TP µg/g', 'Notes']]

        #Rename Fields to match DB Schema
        df_ToAppendFinal.rename(columns={'Bottle weight (g)': 'Bottle_Weight_g', 'Plant weight (g)': 'Plant_Weight_g', 'Sample wet weight (g)': 'Sample_Wet_Weight_g',
                                    'TP µg/g':'Total_Phosphorus'},inplace=True)


        #Round Total Phosphorus field to 2 decimal - have made the native field string to accommodate Text Code Flags
        #df_ToAppendFinal.round({'Total_Phosphorus': 2})

        #Get Number of Columns
        shapeDf = df_ToAppendFinal.shape
        lenColumns = shapeDf[1]

        #Add 'Event_ID_DummyIndex' Field - to be used as Index value for SQL Alchemy
        #df_ToAppendFinal.insert(lenColumns, 'TotalPhosphorus_Data_ID', df_ToAppendFinal['Event_ID'])
        #Add Index Field - to be used as Index value for SQL Alchemy - Must be Unique Guid to avoid duplicates in table 'tbl_Lab_Data_TotalPhosphorus'
        df_ToAppendFinal['TotalPhosphorus_Data_ID'] = [uuid.uuid4() for x in range(len(df_ToAppendFinal))]

        #Reset the index
        df_ToAppendFinal.reset_index(drop=True, inplace=True)

        # Set Index field to the 'TotalPhosphorus_Data_ID' field - SQL Alchemy will not be able to append to table unless the index field in table 'tbl_Lab_Data_TotalPhosphorus' is defined as the inde
        # this fields index value - value will not be retained on append (i.e. a new autonumber will be populated).
        df_ToAppendFinal.set_index("TotalPhosphorus_Data_ID", inplace=True)

        outFull = workspace + "\DataFrameAppended.csv"
        #Export Data Frame that has been imported
        df_ToAppendFinal.to_csv(outFull, index=True)

        #Create iteration range for records to be appended
        shapeDf = df_ToAppendFinal.shape
        lenRows = shapeDf[0]
        rowRange = range(0, lenRows)

        try:
            for row in rowRange:
                df3 = df_ToAppendFinal[row:row + 1]
                recordIdSeries = df3.iloc[0]
                recordId = recordIdSeries.get('Event_ID')

                #outFull = workspace + "\Test2.csv"
                # # Export
                # df3.to_csv(outFull, index=True)

                appendOut = df3.to_sql(phosphorusTable, con=engine, if_exists='append')
                print(appendOut)
                messageTime = timeFun()
                scriptMsg = "Successfully Appended Event_ID - " + recordId + " - " + messageTime
                print(scriptMsg)
                logFile = open(logFileName, "a")
                logFile.write(scriptMsg + "\n")
                logFile.close()

            return "success function"

        except:

            messageTime = timeFun()
            scriptMsg = "WARNING Failed to Appended Event_ID - " + recordId + " - " + messageTime
            print(scriptMsg)
            logFile = open(logFileName, "a")
            logFile.write(scriptMsg + "\n")
            logFile.close()
            return "failed function"


    except:
        messageTime = timeFun()
        print("Error SFCN_TP_ETL.py  - " + messageTime)
        traceback.print_exc(file=sys.stdout)
        return "Failed function - 'appendRecords'"


if __name__ == '__main__':

    # Write parameters to log file ---------------------------------------------
    ##################################
    # Checking for working directories
    ##################################

    if os.path.exists(workspace):
        pass
    else:
        os.makedirs(workspace)

    #Check for logfile

    if os.path.exists(logFileName):
        pass
    else:
        logFile = open(logFileName, "w")    #Creating index file if it doesn't exist
        logFile.close()

    # Analyses routine ---------------------------------------------------------
    main()
