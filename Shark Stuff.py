#!/usr/bin/env python
# coding: utf-8

# In[20]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[ ]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()


# In[5]:


sharkfile = r'c:\data\GSAF5.csv'


# In[12]:


attack_dates =[]
case = []
country = []
activity =[]
age = []
gender = []
isfatal = []
with open(sharkfile) as f:
        reader = csv.DictReader(f)
        for row in reader:
            case.append(row['Case Number'])
            attack_dates.append(row['Date'])
            country.append(row['Country'])
            activity.append(row['Activity'])
            age.append(row['Age'])
            gender.append(row['Sex '])
            isfatal.append(row['Fatal (Y/N)'])


# In[13]:


data = zip(attack_dates, case, country, activity, age, gender, isfatal)


# In[ ]:


cur.execute(truncate table neil.shark)


# In[19]:


import pyodbc
conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()
q = 'insert into neil.shark (attack_date, case_number, country, activity, age, gender, isfatal) values (?, ?, ?, ?, ?, ?, ?)'


# In[18]:


for d in data:
    try:
        cur.execute (q,d)
        conn.commit()
    except:
        conn.rollback()


# In[ ]:





# In[9]:


isfatal[:15]


# In[10]:


attack_dates[:15]


# In[22]:


datecount = {}
for d in attack_dates:
    datecount[d] = datecount.get(d,0)+1


# In[18]:


sorted(datecount.items(), key=lambda x: x[1], reverse=True)   


# In[27]:


clean_dates=[]
for d in attack_dates:
    try:
        clean_dates.append(datetime.strptime(d.replace('Reported ', ''),'%d-%b-%Y'))
    except:
        pass


# In[30]:


print [clean_dates]


# In[10]:


data = zip(attack_dates, case, country, activity, age, gender, isfatal)


# In[ ]:




