from distutils.command.config import dump_file
from typing import List
from json2html import *
import json

# ttttttttttttttttttttttttttttttttttttttttttttttttttttttttttt



#---------------------------------------------------------------------------------
# данные, используемые программой в SELECT запросе, редактируются при смене задания
#---------------------------------------------------------------------------------
#list_of_fields=("btunit","subunit", "view_market", "view_country", "country")
#list_of_fields=("btunit","subunit","country") #"view_country",
#print(f"тип списка полей: {type(list_of_fields)} поля {list_of_fields}")

#list_of_users=("anna.astafeyeva1@ibm.com", "sandrinechapon@fr.ibm.com") 
#list_of_users=("vladimir.reutovich1@ibm.com", "sandrinechapon@fr.ibm.com") 

#list_of_users=("vladimir.reutovich1@ibm.com", "anna.astafeyeva1@ibm.com") 

select_query="Select userid"
table_name="user_pofile"
schem = "my_test_schema"

#----------------------------------------------------------------------------------------------------------------------
# список используемых json файлов -- данные, используемые программой в SELECT запросе, редактируются при смене задания
#----------------------------------------------------------------------------------------------------------------------
json_file="new1.json"  #файл для промежуточной записи результата
connect_data_file="connection_parameters.json"
list_of_fields_file="fields_for_select.json"
list_of_users_file="list_of_users_file.json"

#---------------------------------------------------------------------------------
# данные подключения к базе данных (редактируюся при изменени базы)
#---------------------------------------------------------------------------------
# d = "postgres"
# # d = "mydatabase1"
# u = "user1"
# p = "_Djkjlz1959_"
# h = "127.0.0.1"
# po = "5432"


# open connection
def open_connection(dt, usr, pas, h, por):
    try:
        import psycopg2
        conn = psycopg2.connect(
            database=dt,
            user=usr,
            password=pas,
            host=h,
            port=por
        )
        #print("Database -- ", dt, " -- opened successfully")
        return conn
    except ConnectionError as e:    # This is the correct syntax
        err=psycopg2.Error
        print(err)
        return


# run select query 
def execute_query(cc, qq):
    import psycopg2
    try:
        cur = cc.cursor()
        cur.execute(qq)
        rows = cur.fetchall()
        return rows
    except psycopg2.Error as err:
        print("---------->>>>", err)
        return None


# run select query without return
def execute_query1(cc, qq):
    cur = cc.cursor()
    cur.execute(qq)
    # rows=cur.fetchall()
    

# close connection
def close_connection(con):
    con.close()
    return


def parse(_in: str) -> List[str]:
    return sorted(_in.split(','))


def ccc(a, b):
    # missing values (exist in the one but not exist in the another one)
    res = list(set(a)-set(b))
    res = repr(res) #преобразование set to string (тогда убирается перевод строки в джейсоне между элементами set)
    
    #print("sets   ", set(a),"        ", set(b))
    #print("rrr=  ",res)
    
    return res


def pr_res(ss, res):
    print(ss, res)
    return


# create main Select query
def create_select_query(list_of_fields, list_of_users, select_query, table_name):
    for a in list_of_fields:
        select_query += ", "+a
    #print(select_query)

    select_query += " from " + table_name

    i = 1
    for a in list_of_users:
        if i < 2:
         select_query += " where userid like(lower(\'%"+a+"%\'))"
        else:
         select_query += " or userid like(lower(\'%"+a+"%\'))"
        i = i+1
        #print(select_query)
    return select_query

def do_mock() -> dict:
    return {'name':"The Lord"}    

def dump_to_file(dictionary: dict, filename: str): 
    with open(filename, "w") as outfile:
        json.dump(dictionary, outfile)  

def read_jason_file(file_name):
    with open(file_name, "r") as read_file:
        json_data = json.load(read_file)
    return json_data

def convert_json_to_html(data):
    json_html=json2html.convert(json=data,table_attributes='border="0"')
    return json_html

def do_work():

    #--------------------------------------------
    # формирование данных для коннекта и коннект к базе
    #--------------------------------------------
    con_set=read_jason_file(connect_data_file)
    db_name=con_set["db"]
    user_name=con_set["username"]
    user_pass=con_set["userpass"]
    host=con_set["host"]
    port=con_set["port"]
    con = open_connection(db_name, user_name, user_pass, host,port)
    #--------------------------------------------
   
    #--------------------------------------------
    # формирование списка полей для исследования
    #--------------------------------------------
    list_of_fields=read_jason_file(list_of_fields_file)
    list_of_fields=list_of_fields["fields"]
    #--------------------------------------------
    
    #--------------------------------------------
    # формирование списка юзеров для исследования
    #--------------------------------------------
    list_of_users=read_jason_file(list_of_users_file)
    list_of_users=list_of_users["users"]
    #--------------------------------------------
    
    #qw1 = "show search_path"
    #rows_qw1 = execute_query(con, qw1)
    # for row in rows_qw1:
    #     print("Old schema is: ",row[0])

    qw2 = "set search_path to my_test_schema"
    execute_query1(con, qw2)   # без return и без rows=cur.fetchall()

    #qw2 = "show search_path"
    #rows = execute_query(con, qw1)
    # for row in rows:
    #     print("New schema is: ",row[0])

    #qw=create_select_query(list_of_fields, list_of_users, select_query, table_name)
    qw=create_select_query(list_of_fields, list_of_users, select_query, table_name)
    rows = execute_query(con, qw)

    comp={}
    cn=0
    #nf=1
    for current_filed_name in list_of_fields:
        cn+=1 #счетчик по полям, принимающим участие в сравнении, начиная с первого
        mylist=[]
        #income = income1 = name = name1 = []
        #pp=номер прохода по юзерам-всего их 2 
        pp=1
        for row in rows:
            #print("\n","---> ",row, "\n", row[0], "\n", row[1], "\n", row[2], "\n")
            if pp==1:
                income = row[cn]
                name=row[0]
                #если данных в поле нет, то income или income1 =''
                if income is None:
                    income=''
            else:
                income1 = row[cn]
                name1 = row[0]
                if income1 is None:
                    income1=''
            pp+=1
        l1 = parse(income)
        l2 = parse(income1)
        main_l1_2 = ccc(l2, l1)
        main_l2_1 = ccc(l1, l2)
        #print(type(main_l1_2))
        a = dict.fromkeys(["user_id"],name)
        a["missing_content"]=[main_l1_2]
        mylist.append(a)
        a = dict.fromkeys(["user_id"],name1)
        a["missing_content"]=[main_l2_1]
        mylist.append(a)
        #print(mylist)
        comp[current_filed_name]=[mylist]
        #print(comp)

        oo = "Original "+name + ": "
        oo1 = "Original "+name1 + ": "
        mm = 'Missing for the ' + name + ' but exist for the ' + name1 + ': '
        mm1 = 'Missing for the ' + name1 + ' but exist for the ' + name + ': '
        print("---------- ", current_filed_name, " ----------------")
        # print("------------------------------------------")
        # pr_res(oo, income)
        # pr_res(oo1, income1)
        # print("------------------------------------------")
        ##pr_res(mm, main_l1_2)
        ##pr_res(mm1, main_l2_1)
        ##print("------------------------------------------")
        #nf+=1

    #----------------------------------------    
    # better to close connection always - in a funally block
    close_connection(con)
    #dictionary = comp
    #записать результат в файл new1.json
    dump_to_file(comp,json_file)
    data_from_json=read_jason_file(json_file)
    json_html=convert_json_to_html(data_from_json)
    return comp #json_html  





if __name__ == '__main__':
    # find out parameters (for instance connection string or table names for comparizn )
    res = do_work()
    #print(f"результат работы ду ворк:{res}")
    dump_to_file(res, json_file)