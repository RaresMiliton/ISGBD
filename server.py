import socket
import json
import redis
from prettytable import PrettyTable
import os
import shutil

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
                    #indexAttributes.append({"attributeName": attribute[0]})
                    #foundIndexes = 1
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
            f = open(
                "databases/" + used_database + "/tables/" + tableName + "/" + tableName + ".kv",
                "w+")
            f.write("key: " + primaryKey[0]["attributeName"] + "\n")
            strA = ""
            for attribute in attributes:
                attribute = attribute.split(' ')
                if attribute[0] == "":
                    attribute = attribute[1:]
                if attribute[0] != primaryKey[0]["attributeName"]:
                    strA += attribute[0] + "#"
            strA = strA[:-1]
            f.write("value: " + strA)
            f.close()
            if uniqueA != "":
                uniqueA = uniqueA[:-1]
                f = open(
                    "databases/" + used_database + "/tables/" + tableName + "/" + "unique" + ".kv",
                    "w+")
                f.write("key: " + uniqueA + '\n')
                f.write("value: " + primaryKey[0]["attributeName"])
                f.close()
            if len(foreignKeys) > 0:
                f = open(
                    "databases/" + used_database + "/tables/" + tableName + "/" + "foreign" + ".kv",
                    "w+")
                f.write("key: " + foreignKeys[0]["foreignKey"] + '\n')
                f.write("value: " + foreignKeys[0]["refTable"] + '#' + foreignKeys[0]["refAttribute"])
                f.close()
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
            del data["databases"][db]
            write_json(data)
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
                    key = used_database+":"+statement[1]+":*"
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
                attributes = statement[2:]
                attributes = parseAttributes(attributes)
                no = 0
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
                            if st[i]["attributeName"] == data["databases"][used_database]["tables"][table_name]["primaryKey"][0]["attributeName"]:
                                key += attributes[i]
                            else:
                                value += attributes[i] + "#"
                    value = value[:-1]
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
                                        serverSocket.sendto("FOREIGN KEY {} DOES NOT EXIST IN TABLE {}".format(attributes[i],fk["refTable"]).encode(), address)
                                        no = 1
                    if len(r.keys(key)) > 0:
                        serverSocket.sendto("DATA WITH THAT PRIMARY KEY ALREADY EXISTS IN TABLE {}".format(table_name).encode(), address)
                    else:
                        if no == 0:
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
                                serverSocket.sendto("ATTRIBUTE {} DOES NOT EXIST IN TABLE {}".format(st.split('=')[0], table_name).encode(), address)
                                break
                            else:
                                attribute_list.append(st.split('=')[0])
                                value_list.append(st.split('=')[1])
                                value_index.append(attPos)
                        print(value_index)
                        for td in table_data:
                            ok = 1
                            td_values = td.decode().split(":")[2] + "#" + r.get(td).decode()
                            td_values = td_values.split("#")
                            print(td_values)
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
    if (statement[0].lower() != "*") or (statement[1].lower() != "from"):
        serverSocket.sendto("INVALID SELECT COMMAND".encode(), address)
    else:
        if used_database:
            table = data["databases"][used_database]["tables"].get(statement[2], None)
            if table:
                attributes = []
                for str in data["databases"][used_database]["tables"][statement[2]]["structure"]:
                    attributes.append(str["attributeName"])
                t = PrettyTable(attributes)
                for key in r.keys(used_database+':'+statement[2]+':*'):
                    values = []
                    values.append(key.decode().split(':')[2])
                    values += r.get(key).decode().split('#')
                    t.add_row(values)
                serverSocket.sendto(t.get_string().encode(), address)
            else:
                msg = "TABLE DOES NOT EXIST"
                serverSocket.sendto(msg.encode(), address)
        else:
            msg = "DATABASE DOES NOT EXIST"
            serverSocket.sendto(msg.encode(), address)


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
    else:
        serverSocket.sendto("INVALID COMMAND".encode(), address)
