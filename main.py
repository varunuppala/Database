import os
import DDL
import DML



def parse_statement(stmt):
    stmt = stmt.replace('(', ' ')
    stmt = stmt.replace(')', '')
    stmt = stmt.replace(',', ' ')
    lst = stmt.split(" ")
    return lst

def exists(table):
    if (os.path.exists("%s.csv" % table)):
        return True;


if __name__ == '__main__':
    statement = input("enter your query:")
    lst = parse_statement(statement)

    while  statement != "exit":
        if lst[0] == "create":
            DDL.Create(lst[1:])
        elif lst[0] == "insert":
            DDL.insert(statement)
        elif lst[0] == "select":
            DML.select(lst[1:],statement)
        elif lst[0] == "delete":
            DDL.delete(statement)
        elif lst[0] == "drop":
            DDL.drop(lst[1:])
        elif lst[0] == "update":
            DDL.update(lst[1:])
        elif lst[0] == "bulk":
            DML.bulk(lst[1:])
        else:
            print("please check your syntax")


        statement = input("enter your query:")
        lst = parse_statement(statement)

    print("Bye Bye!!")


