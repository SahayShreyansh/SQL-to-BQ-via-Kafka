table_dict={
    'emp_manager':["emp_id","mp_name","salary","manager_id"],
    'RankTest1':["Name","Sales","Location"]}

for key in table_dict.values():
    values = table_dict[key]
    print(f"Key: {key}, Values: {values}")
