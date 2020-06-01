
# coding: utf-8

# In[7]:

from pyspark import SparkContext


# In[9]:

sc.stop()


# In[10]:

sc=SparkContext()


# In[11]:

get_ipython().run_cell_magic('writefile', 'example.txt', 'first line\nsecond line\nthird line\nfourth line')


# In[12]:

textFile = sc.textFile('example.txt')


# In[13]:

textFile.count()


# In[14]:

textFile.first()


# In[18]:

secfind = textFile.filter(lambda line:'second' in line)


# In[19]:

secfind


# In[20]:

secfind.count()


# In[21]:

get_ipython().run_cell_magic('writefile', 'example2.txt', 'first\nsecond line\nthe third line\nthen a fourth line')


# In[23]:

from pyspark import SparkContext


# In[24]:

sc.stop()


# In[25]:

sc=SparkContext()


# In[26]:

sc.textFile('example2.txt')


# In[28]:

text_rdd = sc.textFile('example2.txt')


# In[30]:

words=text_rdd.map(lambda line: line.split())


# In[31]:

words.collect()


# In[32]:

text_rdd.collect()


# In[33]:

text_rdd.flatMap(lambda line: line.split()).collect()


# In[34]:

get_ipython().run_cell_magic('writefile', 'services.txt', '#EventId    Timestamp    Customer   State    ServiceID    Amount\n201       10/13/2017      100       NY       131          100.00\n204       10/18/2017      700       TX       129          450.00\n202       10/15/2017      203       CA       121          200.00\n206       10/19/2017      202       CA       131          500.00\n203       10/17/2017      101       NY       173          750.00\n205       10/19/2017      202       TX       121          200.00')


# In[35]:

services = sc.textFile('services.txt')


# In[36]:

services.take(2)


# In[38]:

services.map(lambda line: line.split()).take(3)


# In[47]:

clean =services.map(lambda line:line[1:] if line[0]=='#' else line)


# In[48]:

clean = clean.map(lambda line : line.split())


# In[49]:

clean.collect()


# In[57]:

pairs =clean.map(lambda  lst: (lst[3],lst[-1]))


# In[58]:

pairs.collect()


# In[61]:

rekey = pairs.reduceByKey(lambda amt1, amt2 : float( amt1) + float(amt2))


# In[62]:

rekey.collect()


# In[63]:

clean.collect()


# In[68]:

# Grab (State, Amount)
step1= clean.map(lambda lst: (lst[3],lst[-1]))
# Reduce by key
step2 = step1.reduceByKey(lambda amt1,amt2 : float(amt1)+float(amt2))
# Get rid of state, Amount titles
step3 = step2.filter(lambda x : not x[0]=='State')
# Sort Results by Amount
step4 = step3.sortBy(lambda stAmount:stAmount[1], ascending =False)
#Action
step4.collect()


# In[69]:

x=['ID', 'State' , 'Amount']


# In[70]:

def func1(lst):
    return lst[-1]


# In[71]:

def func2(id_st_amt):
    #unpack values
    (Id,st,amt)= id_st_amt
    return amt


# In[ ]:



