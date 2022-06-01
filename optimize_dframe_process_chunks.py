import pandas as pd, numpy as np
pd.options.display.max_columns = 99

data = pd.read_csv('loans_2007.csv')

# data.iloc[:3100,:].info(memory_usage='deep')
# data.isnull().sum()

# data.info()


chunks = pd.read_csv('loans_2007.csv', chunksize = 3100) 

ls = []
n_chunk = 0

data.select_dtypes(include=['object']).tail(3)

data['id'] = data['id'].drop(labels = [42536,42537])

print(data.tail(4))

floats_miss_value = []
total_mem_usage_before = []
total_mem_usage_after = [] 
for chunk in chunks:
#     print(chunk.dtypes.value_counts())
    n_chunk += 1
    total_mem_usage_before.append(chunk.memory_usage(deep = True).sum() / (1024*1024))
    for col in chunk.select_dtypes(include=['object']):
#         print(n_chunk)
        if len(chunk[col].unique()) / len(chunk) < 0.5:
#             print(col)
#             print(chunk[col].memory_usage(deep = True))
            chunk[col] = chunk[col].astype('category')
#             print(chunk[col].memory_usage(deep = True))
            
#             print(col,' is less unique and has: ',len(chunk[col].unique()),'Vs', len(chunk[col]))
#         else:
#             print(col,': ',len(chunk[col].unique()))
    
    chunk['int_rate'] = data['int_rate'].str.rstrip('%')
#     print('Before: ',chunk['int_rate'].memory_usage(deep = True))
    chunk['int_rate'] = chunk['int_rate'].astype('float')
    chunk['int_rate'] = pd.to_numeric(chunk['int_rate'], downcast = 'float')
#     print('After: ',chunk['int_rate'].memory_usage(deep = True))
    for col in chunk.select_dtypes(include=['float']):
        if chunk[col].isnull().sum() == 0:
            floats_miss_value.append([col, chunk[col]])
    total_mem_usage_after.append(chunk.memory_usage(deep=True).sum() / (1024*1024))
#     chunk['loan_amnt'] = chunk['member_id'].astype('int')
#     chunk['member_id'] = pd.to_numeric(chunk['member_id'], downcast = 'integer')    
floats_miss_value
# print(total_mem_usage_before)
# print(total_mem_usage_after)


def opt_size(chunk_size):
    rows = 100         
    while chunk_size > (pd.read_csv('loans_2007.csv', nrows = rows).memory_usage(deep = True).sum()) / (1024 ** 2):
        rows += 100
        n_data = pd.read_csv('loans_2007.csv', nrows = rows) 
