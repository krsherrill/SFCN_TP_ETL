# SFCN_TP_ETL
The Github **SFCN-TP_ETL** repository contains the python script (i.e. SFCN_TP_ETL.py) which performs Extract Transform and Load (ETL) of the total phosphorus Electronic Data Deliverable (EDD) for the Periphtyon monitoring protocol at the South Florida Caribbean Network, Inventory and Monitoirng Program of the National Park Service.  The lab EDD is ETL to the Periphtyon Access database table **tbl_Lab_Data_TotalPhosphorus**. 

As of the winter of 2023 Total Phosphorus lab analysis has been performed by the Florida International University Southeast Environmental Research Center (FIA SERC) laboratory.  The Total Phosphorus ETL scripting and accompanying workflow has been specifically designed to accommodate the EDD provided by FIA SERC.  

## SFCN_TP_ETL.py

**Extracts the Data records from the FIA SERC EDD.**

**Defines Matching Site/Events for South Florida Caribbean Network Periphyhton monitoirng done by hydrological year/field season** in the Periphyton access database.

***Script Parameters***

*inputFile* - Excel EDD from from the lab to be processed

*rawDataSheet* - Name of the raw data sheet in the inputFile parameter.

*firstRow* - Defines the Text value in the First Row and First Column (i.e. farthest left of table) of the 'datasheet' field sheet that should be retained.  Being used to remove header rows.

*hydroYear* - Hydrological year/field season for which processing is occurring.

*inDB* – Path to the Periphyton Access database

**Appends the transformed total phosphorus data (i.e. ETL)** to the Periphyton dataset **tbl_SoilChemistry_Dataset** via the 'to_sql' functionality for dataframes in the [sqlAlchemy-access 2.0.1](https://pypi.org/project/sqlalchemy-access/) package. Install via pip install sqlalchemy-access in your python environment.
