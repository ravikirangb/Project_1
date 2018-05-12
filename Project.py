# Project Assignment:

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

df= pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
print (df.head(2))

df1 = pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
print (df1.head(2))

# 1. Get the Metadata from the above files.
print ("******* #1 df info : \n\n")

print (df.info())
print ("*******# 1 df1 info : \n\n")
print (df1.info())

# 2. Get the row names from the above files.
print ("*******# 2 Row name Info df: \n\n")
print (df.index.values)
print ("*******# 2 Row name Info df1 : \n\n")
print (df1.index.values)

# 3. Change the column name from any of the above file.
print ("*******# 3 Change the column name from any of the above file : \n\n")
print(df.rename(columns={'Indicator': 'Indicator_id'}).head())

#4 4. Change the column name from any of the above file and store the changes made permanently.
df = df = df.rename(columns={'Indicator': 'Indicator_id'})
print ("*******# 4 Change the column name from any of the above file and store the changes made permanently : \n\n")
print (df.head(2))

# 5. Change the names of multiple columns.

df = df.rename(columns={'PUBLISH STATES': 'Publication Status', 'WHO region': 'WHO Region'})
print ("*******# 5. Change the names of multiple columns.: \n\n")
print (df.head(2))

# 6. Arrange values of a particular column in ascending order.
print ("*******# 6. Arrange values of a particular column in ascending order.: \n\n")
print (df.sort_values(by=['Year'], ascending=True).head(10))

# 7. Arrange multiple column values in ascending order.
print ("*******# 7. Arrange multiple column values in ascending order.: \n\n")
print (df.sort_values(by=['Indicator_id','Country','Year','WHO Region','Publication Status']).head(3))

# 8. Make country as the first column of the dataframe.
df = df.reindex(['Country', 'Indicator_id', 'Publication Status', 'Year', 'WHO Region', 'World Bank income group','Sex','Display Value','Numeric', 'Low','High','Comments'], axis=1)
print ("*******# 8. Make country as the first column of the dataframe: \n\n")
print (df.head(5))

# 9. Get the column array using a variable

WHORegion_Array = df["WHO Region"].values

print ("****# 9. Get the column array using a variable \n\n ",WHORegion_Array)

       
# 10. Get the subset rows 11, 24, 37
print ("10. Get the subset rows 11, 24, 37 : \n",df.loc[[11, 24, 37], :])

       
#11. Get the subset rows excluding 5, 12, 23, and 56

exclude5_12_23_56 = df.index.isin([5, 12, 23, 56])
print ("#11. Get the subset rows excluding 5, 12, 23, and 56\n\n",df[~exclude5_12_23_56].head(56))

       
# Load datasets from CSV
users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')

print ("Users:: \n\n",users.head())
print ("sessions:: \n\n",sessions.head())
print("transactions:: \n\n",transactions.head())

#12. Join users to transactions, keeping all rows from transactions and only matching rows from users (left join)

transactions.merge(users, how='left', on='UserID')
      
# 13. Which transactions have a UserID not in users?
transactions[~transactions['UserID'].isin(users['UserID'])]  

#14. Join users to transactions, keeping only rows from transactions and users that match via UserID (inner join)
transactions.merge(users, how="inner", on='UserID')

# 15. Join users to transactions, displaying all matching rows AND all non-matching rows (full outer join)

transactions.merge(users, how="outer", on='UserID')

# 16. Determine which sessions occurred on the same day each user registered

users.merge(sessions, left_on=['UserID', 'Registered'], right_on=['UserID', 'SessionDate'])

# 17. Build a dataset with every possible (UserID, ProductID) pair (cross join)

Users = users
Users['key'] = 0

Products = products
Products['key'] = 0

pd.merge(Users, Products, on='key', how="outer")[['UserID', 'ProductID']]

# 18. Determine how much quantity of each product was purchased by each user 

users.merge(products, how='outer').merge(transactions, on=['UserID','ProductID'], how="outer").loc[:, ["UserID", "ProductID", "Quantity"]].fillna(0)


# 19. For each user, get each possible pair of pair transactions (TransactionID1,TransacationID2) 

pd.merge(transactions, transactions, on='UserID')

# 20. Join each user to his/her first occuring transaction in the transactions table 

data = pd.merge(users, transactions.groupby('UserID').first().reset_index(), how='left', on='UserID')

print("#20 :: \n\n",data)


# 21. Test to see if we can drop columns 

my_columns = list(data.columns)

print("::::",my_columns)


list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns) #set threshold to drop NAs



missing_info = list(data.columns[data.isnull().any()])
print("::::",missing_info)



print("Count of missing data:\n")

for col in missing_info:
    num_missing = data[data[col].isnull() == True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing))



print("Percentage of missing data:\n")

for col in missing_info:
    percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0]
    print('percent missing for column {}: {}'.format(col, percent_missing))
       
       
       