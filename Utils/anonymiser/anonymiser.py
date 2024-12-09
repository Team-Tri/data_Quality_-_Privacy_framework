from src.Utils.HelperFunction.helper import encrypt_data
from faker import Faker
import ast
fake = Faker()

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
            print(key,encryption_type)
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