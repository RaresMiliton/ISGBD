import socket
import json
import redis

ip_address = "127.0.0.1"
port = 9999
bufferSize = 1024

serverSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
serverSocket.bind((ip_address, port))

r = redis.Redis()


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


def create(statement):
    data = open_file()
    if statement[0].lower() == "database":
        if data == {}:
            data = {"databases": {}}
        data["databases"][statement[1]] = ({"name": statement[1], "tables": {}})
        write_json(data)
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
            for attribute in attributes:
                isNull = '1'
                attribute = attribute.split(' ')
                if attribute[0] == "":
                    attribute = attribute[1:]  # check for white spaces after comma
                if "primary" in attribute:  # check for any primary key
                    primaryKey.append({"attributeName": attribute[0]})
                    uniqueKeys.append({"attributeName": attribute[0]})
                    indexAttributes.append({"attributeName": attribute[0]})
                    foundIndexes = 1
                    isNull = '0'
                if "unique" in attribute:  # check for any unique key
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
    if (statement[0] != "into") or (len(statement) < 3):
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
                str = ""
                for attribute in attributes:
                    str += attribute
                attributes = str.split(" ")
                if len(attributes) != data["databases"][used_database]["tables"][table_name]["rowLength"]:
                    serverSocket.sendto("THE NUMBER OF ATTRIBUTES DIFFER!".encode(), address)
                else:
                    st = data["databases"][used_database]["tables"][table_name]["structure"]
                    for i in range(len(attributes)):
                        if not compareType(attributes[i], st[i]["type"], st[i]["length"]):
                            serverSocket.sendto("ATTRIBUTE TYPES DO NOT MATCH!".encode(), address)
                            break
                        else:
                            if st[i]["attributeName"] == data["databases"][used_database]["tables"][table_name]["primaryKey"][0]["attributeName"]:
                                key += attributes[i]
                            else:
                                value += attributes[i] + "#"
                    value = value[:-1]
                    print(key, value)
                    r.set(key, value)
                    serverSocket.sendto("DATA INSERTED INTO {}".format(table_name).encode(), address)

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

    if clientData[0].lower() in ["create", "drop", "use", "insert", "delete"]:
        func = locals()[clientData[0].lower()]
        del clientData[0]
        func(clientData)
    elif clientData[0].lower() == "exit":
        serverSocket.sendto("BYE!".encode(), address)
    else:
        serverSocket.sendto("INVALID COMMAND".encode(), address)
