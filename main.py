# %%
import pandas as pd
import sqlalchemy as sa
from src.Utils.HelperFunction.helper import fetchDataMssql,encrypt_data,generate_deterministic_key,generate_non_deterministic_key
from src.Utils.connectors.connector import connectMssql
import json
import re ,ast
from faker import Faker
import numpy as np

import datetime

fake = Faker()

# %%
import os

# %%
os.curdir

# %%
with open(r"\Users\dhanush.shetty\DContracts_DQP\src\config\config.json","r") as j:
    conf_=json.load(j)

# %%
conf_

# %%
access_=pd.read_csv(r"C:\Users\dhanush.shetty\presidio_demo\data\Access_table.csv")
  

# %%
classified_df=pd.read_csv(r"C:\Users\dhanush.shetty\presidio_demo\data\Classified_metadata.csv")

# %%
def anonymize(value, rule, tag ):
    method_=[x for x in rule.keys()][0]
    if str(value) !="nan" :
        if method_ == "keep":
            if rule[method_] == "yes":
                return value
        elif str(tag) == "nan" and method_ == "encrypt" :
            key, encryption_type = rule[method_].split(":::")
            # print(key,encryption_type)
            return encrypt_data(key,value,en_type=encryption_type)
        elif method_ == "replace" and tag == "Person" :
            total_words_length = len(str(value).split(" "))
            return " ".join(str(fake.first_name() + " " + fake.last_name()).split(" ")[:total_words_length])
        elif method_ == "replace" and tag == "Email":
            return fake.email()
        elif method_ == "replace" and tag == "Phone":
            return fake.phone_number(format="+## (###) ###-####")
        elif method_ == "encrypt":
            key, encryption_type = rule[method_].split(":::")
            # print(key,encryption_type)
            return encrypt_data(key,value,en_type=encryption_type)
        elif method_ == "mask" and rule[method_].startswith("last"):
            parts = rule[method_].split(":::")
            if parts[1].isnumeric():
                num_chars = int(parts[1])
                mask_char = parts[2]
                return value[:num_chars] + mask_char * (len(value) - num_chars)
            else:
                value_list = value.split(parts[1])
                num_chars=len(value_list[1])
                mask_char = parts[2] 
                value_list[1] = mask_char * num_chars
                return f"{parts[1]}".join(value_list)
        elif method_ == "mask" and rule[method_].startswith("first"):
            parts = rule[method_].split(":::")
            if parts[1].isnumeric():
                num_chars = int(parts[1])
                mask_char = parts[2]
                return  mask_char * (len(value) - num_chars) + value[num_chars:]
            else:
                value_list = value.split(parts[1])
                num_chars=len(value_list[0])
                mask_char = parts[2]
                value_list[0] =  mask_char * num_chars
                return f"{parts[1]}".join(value_list)



        else:
            return None

# %%
classified_df["Column Name"].isin(conf_["Dhanush"]["Columns_to_retain"])

# %%
for user_ in conf_:
    SERVER_=conf_[user_].get("SERVER_NAME")
    DATABASE_=conf_[user_].get("DATABASE_NAME")
    SCHEMA_=conf_[user_].get("SCHEMA_NAME")
    PASSWORD_=conf_[user_].get("PASSWORD")
    #get tables
    for table_ in conf_[user_].get("TABLE_REQUIRED"):
        #form connection
        conn_ = connectMssql(SERVER_,DATABASE_,user_,PASSWORD_,SCHEMA_,"sqlalchemy")
        # check user access to table
        if user_ in str(access_["USER_ACCESS"][access_["Table Name"]==table_].values):
            #fetch data into df
            data_ = fetchDataMssql(conn_,table_,"*")
            #columns 
            columns_name=list(classified_df[classified_df["Table Name"]==table_]["Column Name"])
            data_df =  pd.DataFrame.from_records(data_,columns=[columns_name])
            
            data_df.to_csv(f"C:\\Users\\dhanush.shetty\\presidio_demo\\data\\data_{table_}.csv",index=False)
            data_df = pd.read_csv(f"C:\\Users\\dhanush.shetty\\presidio_demo\\data\\data_{table_}.csv")
            columns_to_retain = conf_[user_]["Columns_to_retain"]
            columns_to_discard = conf_[user_]["Columns_to_discard"]
            columns_to_custom_anonymise = list(conf_[user_]["Columns_for_custom_anonymise"].keys())
            #iterate column and tag from classified data
            for column_,tag_ in classified_df[["Column Name","Tag"]][((classified_df["Table Name"]==table_)\
                                                                     & \
                                                                       ((classified_df["Pii"]=="Yes") \
                                                                        | (classified_df["Column Name"].isin(columns_to_retain) \
                                                                        | (classified_df["Column Name"].isin(columns_to_custom_anonymise) )))\
                                                                     & \
                                                                        (~classified_df["Column Name"].isin(columns_to_discard)))\
                                                                    ].values:
                
                    #seperate rules from config 
                    if column_ not in  columns_to_custom_anonymise :
                        #skip column with no tags
                        if str(tag_) != "nan":
                            rule_ = conf_[user_].get(f"{tag_}")
                            #debug for checking values
                            # print(user_,column_,tag_,rule_)
                            #check if rules if encryption is asked to register keys and user into log for security purpose
                            if [x for x in rule_.keys()][0] == "encrypt":
                                # check if its deterministic or non deterministic type of encryption as both requires different keys
                                #debug
                                print("yes")
                                if rule_["encrypt"] == "Deterministic":
                                    key_ = generate_deterministic_key(f"{user_}_{datetime.datetime.now()}")
                                    rule_["encrypt"]=f"{key_}:::Deterministic"
                                elif rule_["encrypt"] == "Non-Deterministic":
                                    key_ = generate_non_deterministic_key()
                                    print(type(key_))
                                    rule_["encrypt"]=f"{key_}:::Non-Deterministic"
                                print(rule_)
                                #write a log for encryption 
                                pd.DataFrame([[user_,DATABASE_,table_,column_,rule_["encrypt"].split(":::")[0],rule_["encrypt"].split(":::")[1],datetime.datetime.now()]],columns=["user","database","table","column","key","type","time"]).to_csv("Encrypt_log.csv",mode="a",index=False)
                            
                            #apply given rules from config to data
                            data_df[column_] = data_df[column_].apply(lambda x : anonymize(str(x),rule_,tag_))
                        else:
                            continue
                    else:
                        rule_ = conf_[user_]["Columns_for_custom_anonymise"][column_]
                        #debug for checking values
                        # print(user_,column_,tag_,rule_)
                        #check if rules if encryption is asked to register keys and user into log for security purpose
                        if [x for x in rule_.keys()][0] == "encrypt":
                            # check if its deterministic or non deterministic type of encryption as both requires different keys
                            #debug
                            print("yes")
                            if rule_["encrypt"] == "Deterministic":
                                key_ = generate_deterministic_key(f"{user_}_{datetime.datetime.now()}")
                                rule_["encrypt"]=f"{key_}:::Deterministic"
                            elif rule_["encrypt"] == "Non-Deterministic":
                                key_ = generate_non_deterministic_key()
                                print(type(key_))
                                rule_["encrypt"]=f"{key_}:::Non-Deterministic"
                            print(rule_)
                            #write a log for encryption 
                            key,type_ = rule_["encrypt"].split(":::")
                            pd.DataFrame([[user_,DATABASE_,table_,column_,key,type_,datetime.datetime.now()]],columns=["user","database","table","column","key","type","time"]).to_csv("Encrypt_log.csv",mode="a",index=False)
                        
                        #apply given rules from config to data
                        data_df[column_] = data_df[column_].apply(lambda x : anonymize(str(x),rule_,tag_))
                  
            #write data into csv      
            data_df.to_csv(f"{user_}_{table_}_19.csv",index=False)
            print(f"table {table_} created")  
        else:
            print(f"user {user_} has no access table {table_}")
        

# %%


# %%



