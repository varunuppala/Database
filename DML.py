
import pandas as pd
import main
import DDL
import numpy as np
from collections import defaultdict



def select(l,stmt):

    for i in range(1):
        if(l[0] == "*"):

            if "where" in l:
                where(l)
                break


            if "join" in l:
                join(stmt)
                break


            else:
                df = pd.read_csv("%s.csv"%l[-1])
                print(df)
                break


        if ("group" in l):
            groupby(stmt)
            break


        if ("order" in l):
            order_by(stmt)
            break


        if "where" in l:
            operates= ["sum","count","avg"]
            for i in operates:
                if i == l[0]:
                    where_operations(l,stmt)
                    break

            else:
                where(l)
                break




        operate = ["max", "min", "sum", "count", "avg"]

        if l[0] in operate:
            for i in operate:
                if i == l[0]:
                    if "where" not in l:
                        operations(stmt)
                        break



        if l[0]!="*":
            columns = []
            if (main.exists(l[-1])):
                for i in l:
                    if (i == "from"):
                        break;
                    else:
                        if (i != ','):
                            columns.append(i)
                df = pd.read_csv("%s.csv" % l[-1], usecols=columns)
                print(df)



def where_operations(l,stmt):
    tbname = l[l.index("from")+1:l.index("where")]
    condition = l[l.index("where") + 1:]
    where, ops = DDL.where_condition(condition)
    df = pd.read_csv("%s.csv" % tbname[0])
    index = DDL.location(df, where, ops)
    df_name = df.iloc[index]
    op = []
    cols = []
    op.append((stmt[stmt.find("select") + 7:stmt.find("(")]))
    cols.append((stmt[stmt.find("(") + 1:stmt.find(")")]))
    df_final = df_name[cols]
    values = df_final.to_dict()
    res = {}
    for i in range(len(op)):
        count = 0
        for key, val in values.items():
            if op[i] == "sum":
                res["sum_%s" % key] = sum(values[key].values())
            elif op[i] == "avg":
                s = sum(values[key].values())
                for i in values[key].values():
                    count += 1
                res["average_%s" % key] = s / count
            elif op[i] == "count":
                for i in values[key].values():
                    count += 1
                res["count_%s" % key] = count

    print(res)



def operations(stmt):
            op = []
            cols = []
            op.append((stmt[stmt.find("select") + 7:stmt.find("(")]))
            cols.append((stmt[stmt.find("(") + 1:stmt.find(")")]))
            tbname = stmt[stmt.find("from") + 5:]
            df = pd.read_csv("%s.csv" % tbname, usecols=cols)
            values = df.to_dict()
            res = {}
            for i in range(len(op)):
                count = 0
                for key, val in values.items():
                    if op[i] == "max":
                        max_val = max(values[key].values())
                        res["max_%s"%key] = max_val
                    elif op[i] == "min":
                        min_val = min(values[key].values())
                        res["min_%s"%key] = min_val
                    elif op[i] == "sum":
                        res["sum_%s"%key] = sum(values[key].values())
                    elif op[i] == "avg":
                        s = sum(values[key].values())
                        for i in values[key].values():
                            count += 1
                        res["average_%s"%key] = s / count
                    elif op[i] == "count":
                        for i in values[key].values():
                            count += 1
                        res["count_%s"%key] = count


            print(res)

def where(l):
    end_index = l.index("from")
    columns = l[:end_index]
    tbname = l[l.index("from")+1:l.index("where")]
    condition = l[l.index("where") + 1:]
    where, op = DDL.where_condition(condition)

    if l[0]=="*":
        df = pd.read_csv("%s.csv"%tbname[0])
        index = DDL.location(df, where, op)
        print(df.iloc[index])

    else :
        df = pd.read_csv("%s.csv"%tbname[0],usecols = columns)
        index = DDL.location(df,where,op)
        print(df.iloc[index])



def groupby(stmt):
    #stmt = "select count(id) from student group by course"

    op_agg = stmt[stmt.find("select") + 7:stmt.find("(")]
    col1 = stmt[stmt.find("(") + 1:stmt.find(")")]
    tbname = stmt[stmt.find("from") + 5: stmt.find("group")]
    col2 = stmt[stmt.find("by")+3:]

    tbname = tbname.replace(" ", "")

    data = pd.read_csv("%s.csv"%tbname)

    groupdict = data.set_index(col1)[col2].to_dict()

    key = groupdict.keys()
    value = groupdict.values()

    result = defaultdict(list)

    for key, val in sorted(groupdict.items()):
        result[val].append(key)

    grouped = str(dict(result))

    grouped_list = []
    for key, value in result.items():
        if op_agg == "count":
            grouped_list.append((key, len([result for result in value if result])))
        elif op_agg == "sum":
            grouped_list.append((key, sum([result for result in value if result])))
        elif op_agg == "avg":
            grouped_list.append((key, (sum([result for result in value if result])/len([result for result in value if result]))))


    np_array = np.array(grouped_list)

    reshaped_array = np.reshape(np_array, (-1, 2))
    df_group = pd.DataFrame(reshaped_array, columns=[col2,op_agg])

    print(df_group)



def order_by(stmt):
    #stmt = "select name from player order by age"
    lst = main.parse_statement(stmt)


    cols = stmt[stmt.find("select") + 7:stmt.find("from")-1]
    tbname = stmt[stmt.find("from") + 5: stmt.find("order")]
    if lst[-1] == "desc":
        col2 = stmt[stmt.find("by") + 3:stmt.find("desc")-1]
    else:
        col2 = stmt[stmt.find("by")+3:]

    tbname = tbname.replace(" ", "")

    data = pd.read_csv("%s.csv"%tbname)

    groupdict = data.set_index(cols)[col2].to_dict()


    sorted_dict = {}
    if lst[-1] == "desc":
        sorted_keys = sorted(groupdict, key=groupdict.get,reverse=True)
    else:
        sorted_keys = sorted(groupdict, key=groupdict.get)

    for w in sorted_keys:
        sorted_dict[w] = groupdict[w]


    df_group = pd.DataFrame(sorted_dict.items(),columns=[cols,col2])
    print(df_group)


def join(stmt):


    col = stmt[stmt.find("select") + 7:stmt.find("from")]
    tb1 = stmt[stmt.find("from") + 5:stmt.find("join")-1]
    tb2 = stmt[stmt.find("join") + 5:stmt.find("on")-1]
    len1=len(tb1)
    len2=len(tb2)
    tb1_col = stmt[stmt.find("on") + 4+len1: stmt.find("=")-1]
    tb2_col = stmt[stmt.find("=") + 3+len2:]
    print(tb1)
    print(tb2)

    # dataframes
    df_tb1 = pd.read_csv('%s.csv'%tb1)
    df_tb2 = pd.read_csv('%s.csv'%tb2)
    # dictionaries
    tb1_dict = df_tb1.to_dict()
    tb2_dict = df_tb2.to_dict()
    # lists
    tb1_l = list(tb1_dict.items())
    tb2_l = list(tb2_dict.items())
    # 1st columns values
    tb1_values = tb1_dict.get(tb1_col)
    tb2_values = tb2_dict.get(tb2_col)

    tb3_dict = tb1_dict.copy()
    tb3_dict.update(tb2_dict)

    join_df = pd.DataFrame.from_dict(tb3_dict)

    indexNames = join_df[(join_df[tb1_col] != join_df[tb2_col])].index
    join_df = join_df[(join_df[tb1_col] == join_df[tb2_col])]
    print(join_df)
    join_df.to_csv("joinedframe.csv")

def bulk(l):
    list1 = []
    n = int(l[-1])
    if l[-2] == "a":
        if n == 1000:
            for i in range(0,n ):
                list1.append((i, i))
        if n== 10000:
            for i in range(0,n ):
                list1.append((i, i))
        if n == 100000:
            for i in range(0,n ):
                list1.append((i, i))
    if l[-2] == "b":
        if n == 1000:
            for i in range(0,n ):
                list1.append((i, 1))
        if n== 10000:
            for i in range(0,n ):
                list1.append((i, 1))
        if n == 100000:
            for i in range(0,n ):
                list1.append((i, 1))


    df = pd.DataFrame(list1, columns=["r1", "r2"])
    print(df)
    df.to_csv("table_%s.csv"%l[-2], index=False)

