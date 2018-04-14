import pandas
import numpy

indp = pandas.read_csv("./industries_short_names.csv")
rac1p = pandas.read_csv("./RAC1P.csv")
cow = pandas.read_csv("./COW.csv", delimiter=':')

indp_dict = dict(zip(indp.Code, indp.Name))
rac1p_dict = dict(zip(rac1p.Id, rac1p.Race))
cow_dict = dict(zip(cow.b, cow['Not Applicable']))

gender_dict = dict()
gender_dict[1] = 'Male'
gender_dict[2] = 'Female'

features = []
features.extend(list(set(indp_dict.values())))

features.extend(rac1p_dict.values())
features.extend(cow_dict.values())
features.extend(gender_dict.values())
print(len(features))

i = 0
# converts the rows into columns, For eg, for CATEGORY_OF_WORK value 1 signifies Private jobs then a new column named Private Jobs is made and the cell in the record is set to 1.

for chunk in pandas.read_csv("./2003_1_reduced_merged.csv", chunksize = 10000):
    chunk = chunk.dropna(subset=['INDUSTRY','CATEGORY_OF_WORK'])
    chunk = chunk.reset_index()
    print("Working on chunk: ",i, " with number of rows: ",len(chunk))
    extra_feature_cols = pandas.DataFrame(0, index=numpy.arange(len(chunk)), columns=features)
    chunk = pandas.concat([chunk, extra_feature_cols], axis=1)

    for index, row in chunk.iterrows():
        if row.INDUSTRY == 'INDUSTRY':
            continue
        try:
            chunk.loc[index, cow_dict[str(int(row['CATEGORY_OF_WORK']))]] = 1
        except:
            pass

        try:
            chunk.loc[index, gender_dict[int(row['GENDER'])]] = 1
        except:
            pass
        try:
            chunk.loc[index, rac1p_dict[row['RACE']]] = 1
        except:
            pass

        if row.HISPANIC == 1:
            chunk.loc[index, 'HISPANIC'] = 0
        else:
            chunk.loc[index, 'HISPANIC'] = 1
        try:
            chunk.loc[index, indp_dict[row['INDUSTRY']]] = 1
        except:
            pass
    
    print("Writing chunk: ",i)
    if i == 0:
        chunk.to_csv("./flattened_2003.csv", index=False)
    else:
        chunk.to_csv("./flattened_2003.csv", index=False, mode = 'a', header=False)
    i += 1

'''chunk = pandas.read_csv("./2002_1_reduced_merged.csv")
extra_feature_cols = pandas.DataFrame(0, index=numpy.arange(len(chunk)), columns=features)
chunk = chunk.reset_index()
chunk = pandas.concat([chunk, extra_feature_cols], axis=1)
for index, row in chunk.iterrows():
    if row.INDUSTRY == 'INDUSTRY':
        continue
    try:
        chunk.loc[index, cow_dict[str(int(row['CATEGORY_OF_WORK']))]] = 1
    except:
        pass
    try:
        chunk.loc[index, gender_dict[int(row['GENDER'])]] = 1
    except:
        pass
    try:
        chunk.loc[index, rac1p_dict[row['RACE']]] = 1
    except:
        pass
    if row.HISPANIC == 1:
        chunk.loc[index, 'HISPANIC'] = 0
    else:
        chunk.loc[index, 'HISPANIC'] = 1
    try:
        chunk.loc[index, indp_dict[row['INDUSTRY']]] = 1
    except:
        pass
        
chunk.to_csv("./flattened_2002.csv", index=False)'''
