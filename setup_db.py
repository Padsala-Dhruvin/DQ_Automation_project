import sqlite3
from tkinter import INSERT
conn = sqlite3.connect('patients.db')
c = conn.cursor()
# c.execute(''' CREATE TABLE patients(
#     patient_id INTEGER,
#     weight_kg REAL
# )''')
c.execute('INSERT INTO patients (patient_id, weight_kg) VALUES (1, 70.5)')
all_patients = [(None,-5),(2,50),(3,80),(4,120),(5,65),(2,80),(None,300),(5,52)]
c.executemany('INSERT INTO patients (patient_id, weight_kg) VALUES (?,?)', all_patients)
conn.commit()
conn.close();