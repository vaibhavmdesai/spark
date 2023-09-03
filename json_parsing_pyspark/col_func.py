import pandas as pd

emp = {'name': 'Ankit', 'gender': 'Male', 'email': 'ankit@gmail.com'}
emp = {"name": ["Ankit", "Rahul", "Priya"], "gender": ["Male", "Male", "Female"],
       "email": ["ankit@gmail.com", "rahul@gmail.com", "priya@gmail.com"]}

df_emp = pd.DataFrame(emp)
print(type(df_emp))