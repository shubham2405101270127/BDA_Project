import happybase

# Connect to HBase
connection = happybase.Connection('localhost')
connection.open()

table_name = "student"

# Create Table (if not exists)
if table_name.encode() not in connection.tables():
    connection.create_table(
        table_name,
        {'info': dict()}
    )

table = connection.table(table_name)

# ---------------- CREATE ----------------
table.put(b'101', {
    b'info:name': b'Rahul',
    b'info:age': b'20',
    b'info:course': b'BCA'
})

table.put(b'102', {
    b'info:name': b'Priya',
    b'info:age': b'21',
    b'info:course': b'MCA'
})

print("\nStudent Records Inserted Successfully!")

# ---------------- READ ----------------
print("\nReading Student 101:")
row = table.row(b'101')
for key, value in row.items():
    print(key.decode(), ":", value.decode())

print("\nAll Students:")
for key, data in table.scan():
    print("\nRow Key:", key.decode())
    for k, v in data.items():
        print(k.decode(), ":", v.decode())

# ---------------- UPDATE ----------------
table.put(b'101', {b'info:age': b'22'})
print("\nStudent 101 Age Updated!")

print("\nUpdated Student 101:")
row = table.row(b'101')
for key, value in row.items():
    print(key.decode(), ":", value.decode())

# ---------------- DELETE COLUMN ----------------
table.delete(b'101', columns=[b'info:age'])
print("\nAge Column Deleted!")

print("\nStudent 101 After Delete:")
row = table.row(b'101')
for key, value in row.items():
    print(key.decode(), ":", value.decode())

# ---------------- DELETE ROW ----------------
table.delete(b'102')
print("\nStudent 102 Deleted!")

print("\nFinal Student Table:")
for key, data in table.scan():
    print("\nRow Key:", key.decode())
    for k, v in data.items():
        print(k.decode(), ":", v.decode())

connection.close()