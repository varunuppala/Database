import os
import pandas as pd
import main




def delete(stmt):



 tbname = stmt[stmt.find("from")+4:stmt.find("where")]
 col = stmt[stmt.find("where") + 5:stmt.find(">") and stmt.find("<") and stmt.find("=")]
 val = stmt[stmt.find(">") and stmt.find("<") and stmt.find("=")+1:]
 op = stmt[stmt.find(">") and stmt.find("<") and stmt.find("=")]

 tbname = tbname.replace(" ", "")
 col = col.replace(" ", "")
 val = val.replace(" ", "")
 op = op.replace(" ", "")

 df = pd.read_csv("%s.csv"%tbname)

 exists = int(val) in df.values
 if op == "=":
   if exists:
     indexNames = df[ (df[col] != int(val))].index
     df = df[(df[col] != int(val))]
     print ("Row(s) deleted successfuly!")
     df.to_csv("%s.csv"%tbname, index = False, header=True)
   else:
     print("No row(s) match!")

 elif op == ">":
    if exists:
       indexNames = df[(df[col] < val)].index
       df = df[(df[col] < int(val))]
       print("Row(s) deleted successfuly!")
       df.to_csv(tbname, index = False, header=True)
    else:
      print("No row(s) match!")

 elif op == "<":
    if exists:
        indexNames = df[(df[col] > val)].index
        df = df[(df[col] > int(val))]
        print("Row(s) deleted successfuly!")
        df.to_csv("%s.csv"%tbname, index = False, header=True)
    else:
       print("No row(s) match!")



def create_table(l):
    if main.exists(l[-1]):
        print("Table already exists!")
    else:
        col = []
        type = []
        new = l[1:]
        for i in range(len(new)):
            if i % 2 != 0:
                type.append(new[i])

            else:
                col.append(new[i])

        df_type = pd.read_csv("datatypes.csv")
        df2 = {'tablename': l[0], 'col1': type[0], 'col2': type[1],'col3':type[2]}
        df_type = df_type.append(df2, ignore_index=True)
        df_type.to_csv("datatypes.csv")

        df = pd.DataFrame(columns=col)
        df.to_csv("%s.csv"%l[0],index = False)
        print("Table successfully created!")

def Create(l):
    if l[0] == "table":
        create_table(l[1:])


def insert(stmt):

    tbname = stmt[stmt.find("into")+4:stmt.find("(")]
    p1 = stmt[stmt.find("(") + 1:stmt.find(") values")]
    p2 = stmt[stmt.find("values (") + 8:-1]

    tbname = tbname.replace(" ", "")
    p1 = p1.replace(" ", "")
    p2 = p2.replace(" ", "")

    p1_l = p1.split(",")
    p2_l = p2.split(",")

    ln = len(p2_l)
    for i in range(ln):
        p2_l[i] = p2_l[i].replace("'", "")
    ln = len(p1_l)
    for i in range(ln):
        p1_l[i] = p1_l[i].replace("'", "")

    d = dict(zip(p1_l, p2_l))

    data = pd.read_csv("%s.csv"%tbname)
    pk_values = list(x for x in data[p1_l[0]])

    pk_v = int(p2_l[0])

    pk_set = set(pk_values)

    if pk_v in pk_set:
     print("Id(s) is already exist.")

    else:
        df = pd.DataFrame()
        i = 0
        for value in p1_l:
            se = pd.Series(p2_l[i])
            df[p1_l[i]] = se.values
            i += 1

        print(df)

        if not os.path.isfile("%s.csv"%tbname):
            print("The table does not exist.")
        else:
            df.to_csv("%s.csv"%tbname,mode='a',header=False, index=False)
            print("Record(s) inserted successfully!")


def drop(l):
    if not main.exists(l[-1]):
        print("Table not found")
    else:
        os.remove("%s.csv" %l[-1])
        print("Table dropped")



def update(l):
    if not main.exists(l[0]):
        print("Table not found")
    else:
        start_index = l.index("set")
        end_index = l.index("where")
        sublist = l[start_index+1:end_index]
        if ',' in sublist:
            ind = sublist.index(',')
            sublist.pop(ind)
        values=[]
        for i in range(len(sublist)):
            if i % 3 != 1:
                values.append(sublist[i])
        values1 =  list(chunk(values))
        data = pd.read_csv("%s.csv"%l[0])
        condition , op = where_condition(l)
        index = location(data,condition,op)
        df_final = updation(data,values1,index)

        df_final.to_csv("%s.csv"%l[0],index = False)



def chunk(lst):
    for i in range(0, len(lst), 2):
        yield lst[i:i + 2]

def where_condition(l):
    where1 = l[-1]
    where2 = l[-3]
    operator = l[-2]
    lh = [where2,where1]
    return lh , operator

def RepresentsInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def location(df,condition,op):
    df = df.infer_objects()

    if RepresentsInt(condition[1]):
        regd_Index=''
        if op == '=':
            regd_Index = df[df[condition[0]] == int(condition[1])].index.tolist()
        elif op == '>=' :
            regd_Index = df[df[condition[0]] >= int(condition[1])].index.tolist()
        elif op == '<=':
            regd_Index = df[df[condition[0]] <= int(condition[1])].index.tolist()

    else :
        regd_Index=''
        if op == '=':
            regd_Index = df[df[condition[0]] == condition[1]].index.tolist()
        elif op == '>=' :
            regd_Index = df[df[condition[0]] >= condition[1]].index.tolist()
        elif op == '<=':
            regd_Index = df[df[condition[0]] <= condition[1]].index.tolist()



    return regd_Index

def updation(df,values,regd_Index):
    for i in values:
        df.loc[regd_Index, i[0]] = i[1]

    return df











