# pip install -q -U google-generativeai
import google.generativeai as genai
import pandas as pd

def get_model(api_key):
  """
  This Function will return an instance of Google Genarativeai class ,
  with connection to gemini-1.5-flash
  """
  genai.configure(api_key = api_key)
  model = genai.GenerativeModel("gemini-1.5-flash")
  return model

def get_dataframe(model,column_name):
  column_name = ','.join(column_name)
  prompt_=f"You are Data privacy Steward ,You have to help me tag below given Columns With \
                        PII Tags ,provide answer in columnar manner with PII tag As 2nd column , Limit\
                        value to PII column Strictly to Quasi/Yes/No where Quasi is not PII but Quasi-Identifier Data.\
                        response would be used in automation,strict instruction for response dont send anything else than csv format,no explanation or notes needed    \
                        Below is the full column names  \
                        {column_name} \
                        Also add a Column named Compliance ,value in compliance column should be in single value recommendation \
                        for which compliance to follow Example like Hippa,GDPR etc  \
                        Add 4th column which gives suggestion for what type of encryption to be done on given \
                        column ,for columns with no suggestion fill with 'Not Applicable\
                        Format for output : [column_name,pii,compliance_to_follow,Recommended_Masking] \
                        Add 5th Column named TAG which gives what domain column is choose from [Person,Date,Location,Email,Phone,Card] \
                        for any other domain not in this list just insert 'Sensitive' if its senstive column else 'None'. \
                        For example columns related Person names would be person,Address would be location\
                        column ,for columns with no suggestion fill with 'Not Applicable\
                        dont truncate response data ,need all values'"
  print(prompt_)
  df_c=model.generate_content(prompt_)
  list1=df_c.text.split("```")[1].replace('"',"").split("\n")[1:-2]
  print(df_c.text)
  # column_name=df_c.text.split("\n")[0]
  print(list1[-5:-1])   #Debugging
  list2=[(x.split(",")[0],x.split(",")[1],x.split(",")[2],x.split(",")[3],x.split(",")[4]) for x in list1 if len(x)>=4]
  df_classified=pd.DataFrame(list2,columns=["Full_Column_Name","Pii","Compliance_to_follow","Recommended_Encryption","Tag"])
  return df_classified,df_c.text


def getClassifiedDf(key,path_to_metadata_csv):
  gem_api_key=key
  model =get_model(gem_api_key)
  df=pd.read_csv(path_to_metadata_csv)
  df["Full_Column_Name"]=df["Table Name"]+"."+df["Column Name"]
  columns=df.Full_Column_Name
  df_classified,_ = get_dataframe(model,columns)
  df_classified_full = pd.merge(df,df_classified,how="inner",on="Full_Column_Name")
  # df_classified.to_csv('meta_classified.csv')
  # print("Done")
  return df_classified_full,_
