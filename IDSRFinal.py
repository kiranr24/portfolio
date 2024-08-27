

import pandas as pd
import numpy as np


# Load the contacts file from Clio
contacts = pd.read_csv('/Users/dataanalystvista/Desktop/scripts/contacts 2024-05-07 16-38-09.csv', low_memory=False)

contacts.rename(columns={'ID': 'clientid'}, inplace=True)


#contacts.rename(columns={'CreatedAt': 'Contact Created Date'}, inplace=True)

# Load the matters file
matters = pd.read_csv('/Users/dataanalystvista/Desktop/scripts/matters 2024-05-07 16-37-48.csv', low_memory=False)
matters.rename(columns={'Client ID': 'clientid'}, inplace=True)
matters.rename(columns={'Created Date': 'Matter Created Date'}, inplace=True)

#Load the legacy counties file
legacycounties = pd.read_csv('/Users/dataanalystvista/Desktop/scripts/LegacyCounties.csv', low_memory=True)
#legacycounties.rename(columns={'CountyImport': 'County (Client\'s Location)'}, inplace=True)

#Load employee emails (used in data portal for identity verification)
employeeEmails = pd.read_csv('/Users/dataanalystvista/Desktop/scripts/EmployeeEmailList.csv', low_memory=True)

#Load DDS Status 
DDSstatus = pd.read_csv('/Users/dataanalystvista/Desktop/scripts/DDS Updates.csv', low_memory=True)

# Merge the files based on clientid
temp = pd.merge(contacts, matters, how='inner', on='clientid')

#Add in supplementary counties from legacy file
idsr = pd.merge(temp,legacycounties, how='left', on='clientid', sort=False)
idsr['MergedCounty'] = idsr['County (Client\'s Location)'].where(idsr['County (Client\'s Location)'].notnull(), idsr['CountyImport'])
#Drop old and duplicate county fields
idsr = idsr.drop(['County (Client\'s Location)','CountyImport', 'County (do not use)'], axis=1)

#Add in employee emails column
employeeEmails.rename(columns={'Name': 'Responsible Attorney'}, inplace=True)
idsr = pd.merge(idsr,employeeEmails, how='left', on='Responsible Attorney', sort=False)

# Remove timestamp from the contact and matter created date columns
idsr.rename(columns={'Matter Created Date': 'Created Date'}, inplace=True)
idsr['Created Date'] = idsr['Created Date'].str[:11]
idsr['Contact Created Date'] = idsr['Contact Created Date'].str[:11]



#drop unused columns to increase efficiency 
idsr = idsr.drop(['Last Modified','Originating Attorney First Name','Originating Attorney Last Name', 'Prefix','Middle Name','Secondary Phone Number', 'Title','Company','Primary Address Country','Best Way to Contact','Secondary Address Street','Secondary Address City','Secondary Address Province/State','Secondary Address Postal/Zip Code', 'Secondary Address Country', 'Best Way to Contact', 'Housing Status','Employment Status', 'Safe to call?', 'Birth Place', 'Programs (while incarcerated)', 'Disc. Sanctions (while inarc\'d)'
,'Custom Number', 'Display Number','Client Reference','User', 'Statute of Limitations Date','Responsible Attorney First Name','Billable','SURVEY Q1- Type of Helper','SURVEY Q3- Suggestions','SURVEY Q2- Toolkit request?','SURVEY Q4- Scale of 1 to 10','SURVEY Q1- Anyone else helping?','#SIGNIFICANT OUTCOMES','#POSITIVE QUOTES','Connect to R&R Staff','Need to Make Phone Calls','Matter Notifications'], axis=1)


#create count of all Advised/Assisted Legal Outcomes

# def legalOutcomesAA (row):
#    tempCount = 0;
#    if 'Advised/Assisted' in str(row['ID-Outcome 1']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['ID-Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['ID-Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['ID-Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['ID-Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Voting- Outcome 1']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Voting- Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Voting- Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['CS and 290 Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['CS and 290 Outcome 2']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['CS and 290 Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['CS and 290 Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['CS and 290 Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Record Cleaning- Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Record Cleaning- Outcome 2']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Record Cleaning- Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Record Cleaning- Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Record Cleaning- Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Employment- Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Employment- Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Employment- Outcome 3']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Employment- Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Employment- Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Housing- Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Housing-Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Housing- Outcome 3']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Housing- Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Housing- Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Family & Children-Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Family & Children-Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Family & Children-Outcome 3']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Family & Children-Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Family & Children-Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Court Ordered Debt Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Court Ordered Debt Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Court Ordered Debt Outcome 3']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Court Ordered Debt Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Court Ordered Debt Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Public Benefit-Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Public Benefit-Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Public Benefit-Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Public Benefit- Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Public Benefit- Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Education Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Education- Outcome 2']): 
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Education Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Education-Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Education-Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Immigration-Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Immigration- Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Immigration- Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Immigration- Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Immigration- Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Criminal Law/Warrants- Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Criminal Law/Warrants- Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Criminal Law/Warrants- Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Criminal Law/Warrants- Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Criminal Law/Warrants- Outcome 5']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Misc./Other Outcome 1']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Misc./Other Outcome 2']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Misc./Other Outcome 3']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Misc./Other Outcome 4']):
#       tempCount += 1
#    if 'Advised/Assisted' in str(row['Misc./Other Outcome 5']):
#       tempCount += 1
#    return tempCount
# idsr['legalOutcomesCountAA'] = idsr.apply (lambda row: legalOutcomesAA(row), axis=1)

# #WIP
# #create count of all Non Advised/Assisted Legal Outcomes
# def legalOutcomesAll (row):
#    tempCount = 0;
#    if 'a' in str(row['ID-Outcome 1']): 
#       tempCount += 1
#    if (row['ID-Outcome 2']):
#       tempCount += 1
#    if '' not in str(row['ID-Outcome 3']):
#       tempCount += 1
#    if '*' in (str(row['ID-Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['ID-Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Voting- Outcome 1']).strip()): 
#       tempCount += 1
#    if len(str(row['Voting- Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Voting- Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['CS and 290 Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['CS and 290 Outcome 2']).strip()): 
#       tempCount += 1
#    if len(str(row['CS and 290 Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['CS and 290 Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['CS and 290 Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Record Cleaning- Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Record Cleaning- Outcome 2']).strip()): 
#       tempCount += 1
#    if len(str(row['Record Cleaning- Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['Record Cleaning- Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Record Cleaning- Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Employment- Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Employment- Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Employment- Outcome 3']).strip()): 
#       tempCount += 1
#    if len(str(row['Employment- Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Employment- Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Housing- Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Housing-Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Housing- Outcome 3']).strip()): 
#       tempCount += 1
#    if len(str(row['Housing- Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Housing- Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Family & Children-Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Family & Children-Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Family & Children-Outcome 3']).strip()): 
#       tempCount += 1
#    if len(str(row['Family & Children-Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Family & Children-Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Court Ordered Debt Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Court Ordered Debt Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Court Ordered Debt Outcome 3']).strip()): 
#       tempCount += 1
#    if len(str(row['Court Ordered Debt Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Court Ordered Debt Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Public Benefit-Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Public Benefit-Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Public Benefit-Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['Public Benefit- Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Public Benefit- Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Education Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Education- Outcome 2']).strip()): 
#       tempCount += 1
#    if len(str(row['Education Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['Education-Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Education-Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Immigration-Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Immigration- Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Immigration- Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['Immigration- Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Immigration- Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Criminal Law/Warrants- Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Criminal Law/Warrants- Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Criminal Law/Warrants- Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['Criminal Law/Warrants- Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Criminal Law/Warrants- Outcome 5']).strip()):
#       tempCount += 1
#    if len(str(row['Misc./Other Outcome 1']).strip()):
#       tempCount += 1
#    if len(str(row['Misc./Other Outcome 2']).strip()):
#       tempCount += 1
#    if len(str(row['Misc./Other Outcome 3']).strip()):
#       tempCount += 1
#    if len(str(row['Misc./Other Outcome 4']).strip()):
#       tempCount += 1
#    if len(str(row['Misc./Other Outcome 5']).strip()):
#       tempCount += 1
#    return tempCount
# idsr['legalOutcomesAll'] = idsr.apply (lambda row: legalOutcomesAll(row), axis=1)


#Rearrange columns
#cols = list(idsr.columns.values)
#print(cols)
#idsr = idsr[['mean', '0', '1', '2', '3']]

#Regions
BayArea = {
    'CA - Alameda County',
    'CA - Contra Costa County',
    'CA - Marin County',
    'CA - Napa County',
    'CA - San Francisco County',
    'CA - San Mateo County',
    'CA - Santa Clara County',
    'CA - Solano County',
    'CA - Sonoma County'
}

CentralCoast = {
    'CA - Monterey County',
    'CA - San Luis Obispo County',
    'CA - Santa Cruz County'
}

CentralValley = {
    'CA - Alpine County',
    'CA - Amador County',
    'CA - Calaveras County',
    'CA - El Dorado County',
    'CA - Fresno County',
    'CA - Inyo County',
    'CA - Kern County',
    'CA - Kings County',
    'CA - Madera County',
    'CA - Mariposa County',
    'CA - Merced County',
    'CA - Mono County',
    'CA - Sacramento County',
    'CA - San Benito County',
    'CA - San Joaquin County',
    'CA - Stanislaus County',
    'CA - Tulare County',
    'CA - Tuolumne County',
    'CA - Yolo County'
}

SouthCarolina = {
    'SC - Abbeville County',
    'SC - Aiken County',
    'SC - Allendale County',
    'SC - Anderson County',
    'SC - Bamberg County',
    'SC - Barnwell County',
    'SC - Beaufort County',
    'SC - Berkeley County',
    'SC - Calhoun County',
    'SC - Charleston County',
    'SC - Cherokee County',
    'SC - Chester County',
    'SC - Chesterfield County',
    'SC - Clarendon County',
    'SC - Colleton County',
    'SC - Darlington County',
    'SC - Dillon County',
    'SC - Dorchester County',
    'SC - Edgefield County',
    'SC - Fairfield County',
    'SC - Florence County',
    'SC - Georgetown County',
    'SC - Greenville County',
    'SC - Greenwood County',
    'SC - Hampton County',
    'SC - Horry County',
    'SC - Jasper County',
    'SC - Kershaw County',
    'SC - Lancaster County',
    'SC - Laurens County',
    'SC - Lee County',
    'SC - Lexington County',
    'SC - Marion County',
    'SC - Marlboro County',
    'SC - McCormick County',
    'SC - Newberry County',
    'SC - Oconee County',
    'SC - Orangeburg County',
    'SC - Pickens County',
    'SC - Richland County',
    'SC - Saluda County',
    'SC - Spartanburg County',
    'SC - Sumter County',
    'SC - Union County',
    'SC - Williamsburg County'
    'SC - York County'
}

NorCal = {
    'CA - Butte County',
    'CA - Colusa County',
    'CA - Del Norte County',
    'CA - Glenn County',
    'CA - Humboldt County',
    'CA - Lake County',
    'CA - Lassen County',
    'CA - Mendocino County',
    'CA - Modoc County',
    'CA - Nevada County',
    'CA - Placer County',
    'CA - Plumas County',
    'CA - Shasta County',
    'CA - Sierra County',
    'CA - Siskiyou County',
    'CA - Sutter County',
    'CA - Tehama County',
    'CA - Trinity County',
    'CA - Yuba County'
}

SoCal = {
    'CA - Imperial County',
    'CA - Los Angeles County',
    'CA - Orange County',
    'CA - Riverside County',
    'CA - San Bernardino County',
    'CA - San Diego County',
    'CA - Santa Barbara County',
    'CA - Ventura County'
}

OutOfState = {'Out of State County (please specify)'}

region = {
    'Bay Area': BayArea,
    'Central Coast': CentralCoast,
    'Central Valley': CentralValley,
    'NorCal': NorCal,
    'SoCal': SoCal,
    'South Carolina': SouthCarolina,
    'Out of State': OutOfState
}

# Map regions to county
# d1 = {k: oldk for oldk, oldv in region.items() for k in oldv}
# idsr['Region'] = idsr["County (Client's Location)"].map(d1)

# Write to csv
idsr.to_csv('/Users/dataanalystvista/Desktop/scripts/datapull0507.csv', index = False)

#push the csv to Google Sheets
