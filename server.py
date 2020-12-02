import socket
import json
import redis
from prettytable import PrettyTable
import os
import shutil
import random

ip_address = "127.0.0.1"
port = 9999
bufferSize = 1024

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serverSocket.bind((ip_address, port))

r = redis.Redis('localhost', 6379)


def open_file(json_file_name="Catalog.json"):
    with open(json_file_name) as json_file:
        return json.load(json_file)


def write_json(json_database):
    with open('Catalog.json', 'w') as f:
        json.dump(json_database, f, indent=4)


used_database = ""


def use(statement):
    data = open_file()
    db = data["databases"].get(statement[0], None)
    if db:
        global used_database
        used_database = statement[0]
        msg = "NOW USING DATABASE {}".format(statement[0])
        serverSocket.sendto(msg.encode(), address)
    else:
        msg = "DATABASE DOES NOT EXIST"
        serverSocket.sendto(msg.encode(), address)


def parseAttributes(statement):
    attributes = ''
    for attribute in statement:
        attributes += attribute + ' '
    attributes = attributes[1:-2]  # remove first and final paranthesis
    attributes = attributes.split(',')
    return attributes


def compareType(attribute, value, length):
    if value == "int":
        try:
            int(attribute)
            return True
        except:
            return False
    elif value == "float":
        return isinstance(attribute, float)
    elif value in ["varchar", "char"]:
        if not isinstance(attribute, str):
            return False
        elif len(attribute) > int(length):
            return False
        else:
            return True
    else:
        return isinstance(attribute, bool)


def findAttribute(el, attributes):
    for i in range(len(attributes)):
        if el == attributes[i]["attributeName"]:
            return i
    return -1


def create(statement):
    data = open_file()
    if statement[0].lower() == "database":
        if data == {}:
            data = {"databases": {}}
        data["databases"][statement[1]] = ({"name": statement[1], "tables": {}})
        write_json(data)
        os.makedirs("databases/" + statement[1] + "/tables")
        msg = "CREATED DATABASE {}".format(statement[1])
        serverSocket.sendto(msg.encode(), address)

    elif statement[0].lower() == "table":
        if used_database:
            tableName = statement[1]
            statement = statement[2:]  # remove table and table name from statement
            attributes = parseAttributes(statement)  # separate attributes by comma
            structure = []
            primaryKey = []
            uniqueKeys = []
            indexAttributes = []
            indexFiles = []
            attributesNames = []
            foreignKeys = []
            foundIndexes = 0
            uniqueA = ""
            for attribute in attributes:
                isNull = '1'
                attribute = attribute.split(' ')
                if attribute[0] == "":
                    attribute = attribute[1:]  # check for white spaces after comma
                if "primary" in attribute:  # check for any primary key
                    primaryKey.append({"attributeName": attribute[0]})
                    uniqueKeys.append({"attributeName": attribute[0]})
                    uniqueA += attribute[0] + "#"
                    # indexAttributes.append({"attributeName": attribute[0]})
                    # foundIndexes = 1
                    isNull = '0'
                if "unique" in attribute:  # check for any unique key
                    uniqueA += attribute[0] + "#"
                    uniqueKeys.append({"attributeName": attribute[0]})
                if "not" in attribute:  # check if attribute value is not null
                    isNull = '0'
                if '(' in attribute[1]:  # check if there's a declared length
                    length = attribute[1].split('(', 1)[1]
                    length = length[:-1]
                    type = attribute[1].split('(', 1)[0]
                else:
                    length = "4"
                    type = attribute[1]
                if "references" in attribute:
                    referedTable = attribute[3]
                    referedAttribute = attribute[4][1:-1]
                    tables = data["databases"][used_database]["tables"]
                    if referedTable not in tables:
                        msg = "TABLE NOT FOUND IN GIVEN DATABASE {}".format(used_database)
                        serverSocket.sendto(msg.encode(), address)
                    else:
                        structure1 = data["databases"][used_database]["tables"][referedTable]["structure"]
                        for attribute1 in structure1:
                            attributesNames.append(attribute1["attributeName"])
                        if referedAttribute not in attributesNames:
                            msg = "ATTRIBUTE NOT FOUND IN GIVEN TABLE {}".format(referedTable)
                            serverSocket.sendto(msg.encode(), address)
                        else:
                            foreignKeys.append({"foreignKey": attribute[0], "refTable": referedTable,
                                                "refAttribute": referedAttribute})
                structure.append({"attributeName": attribute[0], "type": type, "length": length, "isNull": isNull})
            if foundIndexes == 1:  # check is there are declared indexes
                indexFiles.append({"indexName": tableName + 'Index', "keyLength": len(tableName), "isUnique": "1",
                                   "indexType": "BTree",
                                   "indexAttributes": indexAttributes})
            table = {"tableName": tableName, "fileName": tableName + ".bin", "rowLength": len(attributes),
                     "structure": structure,
                     "primaryKey": primaryKey,
                     "foreignKeys": foreignKeys,
                     "uniqueKeys": uniqueKeys,
                     "indexFiles": indexFiles}
            data["databases"][used_database]["tables"][tableName] = table
            write_json(data)
            os.mkdir("databases/" + used_database + "/tables/" + tableName)
            msg = "CREATED TABLE {}".format(tableName)
            serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE NOT SELECTED"
            serverSocket.sendto(msg.encode(), address)

    elif statement[0].lower() == "index":
        if used_database:
            indexName = statement[1]
            tableName = statement[3]
            statement = statement[4:]  # remove index name and table name from statement
            indexFiles = data["databases"][used_database]["tables"][tableName]["indexFiles"]
            structure = data["databases"][used_database]["tables"][tableName]["structure"]
            foundAttributes = 1
            indexAttributes = []
            attributesNames = []
            attributes = parseAttributes(statement)  # separate attributes by comma
            for attribute in structure:
                attributesNames.append(attribute["attributeName"])
            for attribute in attributes:
                if attribute[0] == " ":
                    attribute = attribute[1:]  # check for white spaces after comma
                if attribute not in attributesNames:  # check if given attribute exists
                    msg = "ATTRIBUTE NOT FOUND IN GIVEN TABLE {}".format(attribute)
                    serverSocket.sendto(msg.encode(), address)
                    foundAttributes = 0
                else:
                    indexAttributes.append({"attributeName": attribute})
            if foundAttributes == 1:
                indexFiles.append({"indexName": indexName + 'Index', "keyLength": len(indexName), "isUnique": "1",
                                   "indexType": "BTree",
                                   "indexAttributes": indexAttributes})
                data["databases"][used_database]["tables"][tableName]["indexFiles"] = indexFiles
                write_json(data)
                msg = "CREATED INDEX {}".format(indexName)
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE NOT SELECTED"
            serverSocket.sendto(msg.encode(), address)

    else:
        msg = "INVALID ATTRIBUTE FOR CREATE COMMAND"
        serverSocket.sendto(msg.encode(), address)


def drop(statement):
    data = open_file()
    global used_database
    if statement[0].lower() == "database":
        db_data = data["databases"].get(statement[1], None)
        if db_data:
            db = statement[1]
            tables = data["databases"][db]["tables"]
            for table in tables:
                key = used_database + ":" + data["databases"][db]["tables"][table]["tableName"] + ":*"
                keys = r.keys(key)
                for k in keys:
                    r.delete(k)
            del data["databases"][db]
            write_json(data)
            shutil.rmtree("databases/" + db)
            msg = "DROPPED DATABASE {}".format(db)
            serverSocket.sendto(msg.encode(), address)
            if used_database == db:
                used_database = ""
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)

    elif statement[0].lower() == "table":
        if used_database:
            table = data["databases"][used_database]["tables"].get(statement[1], None)
            if table:
                tables = data["databases"][used_database]["tables"]
                cantdrop = '0'
                for auxTable in tables:
                    foreignKeys = data["databases"][used_database]["tables"][auxTable]["foreignKeys"]
                    for fk in foreignKeys:
                        if fk["refTable"] == table["tableName"]:
                            cantdrop = '1'
                if cantdrop == '1':
                    msg = "CAN'T DROP REFERRED TABLE"
                    serverSocket.sendto(msg.encode(), address)
                else:
                    del data["databases"][used_database]["tables"][statement[1]]
                    write_json(data)
                    key = used_database + ":" + statement[1] + ":*"
                    keys = r.keys(key)
                    for k in keys:
                        r.delete(k)
                    shutil.rmtree("databases/" + used_database + "/tables/" + statement[1])
                    msg = "DROPPED TABLE {}".format(statement[1])
                    serverSocket.sendto(msg.encode(), address)
            else:
                msg = "TABLE DOES NOT EXIST"
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)


def insert(statement):
    data = open_file()
    global used_database
    if (statement[0].lower() != "into") or (len(statement) < 3):
        serverSocket.sendto("INVALID INSERT COMMAND".encode(), address)
    else:
        if used_database:
            table = data["databases"][used_database]["tables"].get(statement[1], None)
            if table:
                table_name = statement[1]
                key = used_database + ":" + table_name + ":"
                value = ""
                ikey = ""
                attributes = statement[2:]
                attributes = parseAttributes(attributes)
                no = 0
                ok = 0
                if len(attributes) != data["databases"][used_database]["tables"][table_name]["rowLength"]:
                    serverSocket.sendto("THE NUMBER OF ATTRIBUTES DIFFER!".encode(), address)
                else:
                    st = data["databases"][used_database]["tables"][table_name]["structure"]
                    for i in range(len(attributes)):
                        if attributes[i][0] == " ":
                            attributes[i] = attributes[i][1:]
                        if not compareType(attributes[i], st[i]["type"], st[i]["length"]):
                            serverSocket.sendto("ATTRIBUTE TYPES DO NOT MATCH!".encode(), address)
                            break
                        else:
                            if st[i]["attributeName"] == \
                                    data["databases"][used_database]["tables"][table_name]["primaryKey"][0][
                                        "attributeName"]:
                                key += attributes[i]
                            else:
                                value += attributes[i] + "#"
                    value = value[:-1]
                    if len(r.keys(key)) > 0:
                        serverSocket.sendto(
                            "DATA WITH THAT PRIMARY KEY ALREADY EXISTS IN TABLE {}".format(table_name).encode(),
                            address)
                    else:
                        if len(data["databases"][used_database]["tables"][table_name]["foreignKeys"]) > 0:
                            fk = data["databases"][used_database]["tables"][table_name]["foreignKeys"][0]
                            if len(fk) > 0:
                                fKey = used_database + ":" + fk["refTable"] + ":"
                                for i in range(len(attributes)):
                                    if attributes[i][0] == " ":
                                        attributes[i] = attributes[i][1:]
                                    if st[i]["attributeName"] == fk["foreignKey"]:
                                        fKey += attributes[i]
                                        if len(r.keys(fKey)) == 0:
                                            serverSocket.sendto(
                                                "FOREIGN KEY {} DOES NOT EXIST IN TABLE {}".format(attributes[i], fk[
                                                    "refTable"]).encode(), address)
                                            no = 1
                                if no != 1:
                                    f = open(
                                        "databases/" + used_database + "/tables/" + table_name + "/" + "foreignKey" + ".kv",
                                        "w+")
                                    f.write("key: " + fKey.split(':')[-1] + '\n')
                                    f.write("value: " + fk["refTable"])
                                    f.close()
                        if len(data["databases"][used_database]["tables"][table_name]["uniqueKeys"]) > 1:
                            uk = data["databases"][used_database]["tables"][table_name]["uniqueKeys"][1]
                            if len(uk) > 0:
                                uKey = ""
                                for i in range(len(attributes)):
                                    if attributes[i][0] == " ":
                                        attributes[i] = attributes[i][1:]
                                    if st[i]["attributeName"] == uk["attributeName"]:
                                        f = open(
                                            "databases/" + used_database + "/tables/" + table_name + "/" + "uniqueKey" + ".kv",
                                            "a+")
                                        ok = 0
                                        f.seek(0)
                                        lines = f.readlines()
                                        for j in range(len(lines)):
                                            if lines[j].split(' ')[1][:-1] == attributes[i]:
                                                serverSocket.sendto("UNIQUE KEY CONSTRAINT VIOLATED IN TABLE {}".format(
                                                    table_name).encode(), address)
                                                ok = 1
                                        if ok == 0:
                                            uKey += attributes[i]
                                        f.close()
                                if uKey != "":
                                    f = open(
                                        "databases/" + used_database + "/tables/" + table_name + "/" + "uniqueKey" + ".kv",
                                        "a+")
                                    f.write("key: " + uKey + '\n')
                                    f.write("value: " + key.split(':')[-1] + '\n')
                                    f.close()
                        if len(data["databases"][used_database]["tables"][table_name]["indexFiles"]) > 1:
                            index = data["databases"][used_database]["tables"][table_name]["indexFiles"]
                            for inx in index:
                                iKey = ""
                                for i in range(len(attributes)):
                                    if attributes[i][0] == " ":
                                        attributes[i] = attributes[i][1:]
                                    for inat in inx["indexAttributes"]:
                                        if st[i]["attributeName"] == inat["attributeName"]:
                                                iKey += attributes[i] + ":"
                                iKey = iKey[:-1]
                                f = open(
                                    "databases/" + used_database + "/tables/" + table_name + "/" + inx["indexName"] + ".kv",
                                    "a+")
                                f.write("key: " + iKey + '\n')
                                f.write("value: " + key.split(':')[-1] + '\n' + '\n')
                                f.close()
                        if no == 0 and ok == 0:
                            r.set(key, value)
                            serverSocket.sendto("DATA INSERTED INTO {}".format(table_name).encode(), address)

            else:
                msg = "TABLE DOES NOT EXIST"
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)


def delete(statement):
    data = open_file()
    global used_database
    if (statement[0].lower() != "from") or (len(statement) < 2):
        serverSocket.sendto("INVALID DELETE COMMAND".encode(), address)
    else:
        if used_database:
            table = data["databases"][used_database]["tables"].get(statement[1], None)
            if table:
                table_name = statement[1]
                keys = used_database + ":" + table_name + ":*"
                table_data = r.keys(keys)
                if len(statement) == 2:
                    for t_data in table_data:
                        r.delete(t_data)
                    serverSocket.sendto("ALL DATA REMOVED FROM {}".format(table_name).encode(), address)
                else:
                    if statement[2].lower() != "where":
                        serverSocket.sendto("INVALID DELETE COMMAND".encode(), address)
                    else:
                        statement = statement[3:]
                        statement = list(filter('and'.__ne__, statement))
                        attributes = data["databases"][used_database]["tables"][table_name]["structure"]
                        attribute_list = []
                        value_list = []
                        value_index = []
                        for st in statement:
                            attPos = findAttribute(st.split('=')[0], attributes)
                            if attPos == -1:
                                serverSocket.sendto("ATTRIBUTE {} DOES NOT EXIST IN TABLE {}".format(st.split('=')[0],
                                                                                                     table_name).encode(),
                                                    address)
                                break
                            else:
                                attribute_list.append(st.split('=')[0])
                                value_list.append(st.split('=')[1])
                                value_index.append(attPos)
                        # print(value_index)
                        tables = data["databases"][used_database]["tables"]
                        cantdelete = 0
                        for auxTable in tables:
                            foreignKeys = data["databases"][used_database]["tables"][auxTable]["foreignKeys"]
                            for fk in foreignKeys:
                                if fk["refTable"] == table_name:
                                    valueOfAttribute = ''
                                    for i in range(len(attribute_list)):
                                        if attribute_list[i] == fk["refAttribute"]:
                                            valueOfAttribute = value_list[i]
                                            break
                                    f = open(
                                        "databases/" + used_database + "/tables/" + auxTable + "/" + "foreignKey" + ".kv",
                                        "r+")
                                    lines = f.readlines()
                                    for j in range(len(lines)):
                                        if lines[j].split(' ')[1] == table_name:
                                            if lines[j - 1].split(' ')[1][:-1] == valueOfAttribute:
                                                serverSocket.sendto(
                                                    "CAN'T DELETE ROW FROM TABLE {} - foreign key constraint".format(
                                                        table_name).encode(), address)
                                                cantdelete = 1
                                    f.close()
                        if cantdelete != 1:
                            for td in table_data:
                                ok = 1
                                td_values = td.decode().split(":")[2] + "#" + r.get(td).decode()
                                td_values = td_values.split("#")
                                # print(td_values)
                                for i in range(len(value_list)):
                                    if value_list[i] != td_values[value_index[i]]:
                                        ok = 0
                                if ok == 1:
                                    r.delete(td.decode())
                            serverSocket.sendto("DATA DELETED FROM TABLE {}".format(table_name).encode(), address)

            else:
                msg = "TABLE DOES NOT EXIST"
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)


def select(statement):
    data = open_file()
    global used_database
    if ((statement[0].lower() != "*") and (statement[0].lower() != "distinct")) or (statement[1].lower() != "from"):
        serverSocket.sendto("INVALID SELECT COMMAND".encode(), address)
    else:
        if used_database:
            table = data["databases"][used_database]["tables"].get(statement[2], None)
            if table:
                if len(statement) == 3:
                    attributes = []
                    for str in data["databases"][used_database]["tables"][statement[2]]["structure"]:
                        attributes.append(str["attributeName"])
                    t = PrettyTable(attributes)
                    for key in r.keys(used_database + ':' + statement[2] + ':*'):
                        values = []
                        values.append(key.decode().split(':')[2])
                        values += r.get(key).decode().split('#')
                        t.add_row(values)
                    serverSocket.sendto(t.get_string().encode(), address)
                if len(statement) > 3:
                    attributes = []
                    select_type = statement[0].lower()
                    table = statement[2]
                    for str in data["databases"][used_database]["tables"][table]["structure"]:
                        attributes.append(str["attributeName"])
                    t = PrettyTable(attributes)
                    statement = statement[4:]
                    if "and" in statement:
                        statement.pop(1)
                    index = data["databases"][used_database]["tables"][table]["indexFiles"]
                    ok = 0
                    indexFile = ''
                    for inx in index:
                        if len(statement) == len(inx['indexAttributes']):
                            for s in statement:
                                for ind in inx['indexAttributes']:
                                    if s.split("=")[0] == ind["attributeName"]:
                                        ok += 1
                        if ok == len(statement):
                            indexFile = inx["indexName"] + '.kv'
                            break
                    if ok == len(statement):
                        search_key = ""
                        for s in statement:
                            search_key += s.split('=')[1] + ":"
                        search_key = search_key[:-1]
                        copy = []
                        with open("databases/" + used_database + "/tables/" + table + '/' + indexFile, 'r') as iFile:
                            for line in iFile:
                                if line.startswith("key: " + search_key):
                                    pK = next(iFile, '').strip().split()[1]
                                    key = used_database + ":" + table + ":" + pK
                                    values = [pK]
                                    value = r.get(key).decode().split('#')
                                    values += value
                                    if select_type == "distinct":
                                        if value not in copy:
                                            copy.append(value)
                                            t.add_row(values)
                                    else:
                                        t.add_row(values)
                    sFile = open("databases/select.txt", 'w')
                    sFile.write(t.get_string())
                    sFile.close()
                    serverSocket.sendto("SELECT".encode(), address)
            else:
                msg = "TABLE DOES NOT EXIST"
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)


def generate():
    companie = ["BlueAir", "FlyEmirates", "Wizz", "TurkishAirlines"]
    plecare = ['Spania', 'Anglia', 'Germania', 'Franta', 'Olanda', 'Portugalia', 'Italia']
    destinatie = ['SUA', "Canada", 'Japonia', 'Mexic', "Egipt", 'Inonezia', 'Columbia']
    f = open(
        "databases/d2/tables/Zboruri/i1Index.kv", "a+")
    g = open(
        "databases/d2/tables/Zboruri/i2Index.kv", "a+")
    for i in range(1000000):
        com = random.choice(companie)
        plec = random.choice(plecare)
        dest = random.choice(destinatie)
        k = str(i+1)
        key = "d2:Zboruri:" + k
        value = com + '#' + plec + '#' + dest
        r.set(key, value)
        f.write("key: " + plec + ':' + dest + '\n')
        f.write("value: " + k + '\n' + '\n')
        g.write("key: " + com + '\n')
        g.write("value: " + k + '\n' + '\n')
    f.close()
    g.close()
    serverSocket.sendto("Generated 1 million values for Zboruri".encode(), address)


print("Server Up")

while True:
    clientData, address = serverSocket.recvfrom(bufferSize)

    clientData = clientData.decode().split(" ")

    if clientData[0].lower() in ["create", "drop", "use", "insert", "delete", "select"]:
        func = locals()[clientData[0].lower()]
        del clientData[0]
        func(clientData)
    elif clientData[0].lower() == "exit":
        serverSocket.sendto("BYE!".encode(), address)
    elif clientData[0].lower() == "generate":
        generate()
    else:
        serverSocket.sendto("INVALID COMMAND".encode(), address)
