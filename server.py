import socket
import json
import redis
from prettytable import PrettyTable
import os
import shutil
import random
import string

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
        try:
            float(attribute)
            return True
        except:
            return False
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


def load_index(key, path):
    f = open(path, "w")
    keys = r.keys(key)
    for k in keys:
        ke = k.decode().split(":")
        ks = ""
        for i in range(3, len(ke)):
            ks += ke[i] + ":"
        ks = ks[:-1]
        f.write("key: " + ks + '\n')
        f.write("value: " + r.get(k).decode() + '\n' + '\n')
    f.close()


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
                            print(st[i]["type"])
                            print(attributes[i])
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
                                    f_key = "foreignKey:" + str(used_database) + ":" + str(table_name) + ":" + str(fKey.split(':')[-1])
                                    search_fkey = r.keys(f_key)
                                    if len(search_fkey) > 0:
                                        fvalue = r.get(f_key).decode()
                                        fvalue += "#" + key.split(':')[-1]
                                        r.set(f_key, fvalue)
                                    else:
                                        r.set(f_key, key.split(':')[-1])
                                    load_index("foreignKey:" + str(used_database) + ":" + str(table_name) + ":*", "databases/" + used_database + "/tables/" + table_name + "/" + "foreignKey" + ".kv")
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
                                            if len(lines[j].split(' ')) > 1:
                                                if lines[j].split(' ')[1][:-1] == attributes[i]:
                                                    serverSocket.sendto("UNIQUE KEY CONSTRAINT VIOLATED IN TABLE {}".format(
                                                        table_name).encode(), address)
                                                    ok = 1
                                                    break
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
                        if len(data["databases"][used_database]["tables"][table_name]["indexFiles"]) > 0:
                            index = data["databases"][used_database]["tables"][table_name]["indexFiles"]
                            for inx in index:
                                iKey = inx["indexName"] + ":" + used_database + ":" + table_name + ":"
                                iKey_1 = iKey + "*"
                                for i in range(len(attributes)):
                                    if attributes[i][0] == " ":
                                        attributes[i] = attributes[i][1:]
                                    for inat in inx["indexAttributes"]:
                                        if st[i]["attributeName"] == inat["attributeName"]:
                                            iKey += attributes[i] + ":"
                                iKey = iKey[:-1]
                                search_ikey = r.keys(iKey)
                                if len(search_ikey) > 0:
                                    ivalue = r.get(iKey).decode()
                                    ivalue += "#" + key.split(':')[-1]
                                    r.set(iKey, ivalue)
                                else:
                                    r.set(iKey, key.split(':')[-1])
                                path = "databases/" + used_database + "/tables/" + table_name + "/" + inx[
                                        "indexName"] + ".kv"
                                load_index(iKey_1, path)

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
                    if os.path.exists(
                            "databases/" + used_database + "/tables/" + table_name + "/" + "foreignKey" + ".kv"):
                        os.remove("databases/" + used_database + "/tables/" + table_name + "/" + "foreignKey" + ".kv")
                    if os.path.exists(
                            "databases/" + used_database + "/tables/" + table_name + "/" + "uniqueKey" + ".kv"):
                        os.remove("databases/" + used_database + "/tables/" + table_name + "/" + "uniqueKey" + ".kv")
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
                        cantdelete = 0
                        tables = data["databases"][used_database]["tables"]
                        ftable = ""
                        for auxTable in tables:
                            foreignKeys = data["databases"][used_database]["tables"][auxTable]["foreignKeys"]
                            for fk in foreignKeys:
                                if fk["refTable"] == table_name:
                                    ftable = auxTable
                                    valueOfAttribute = ''
                                    for i in range(len(attribute_list)):
                                        if attribute_list[i] == fk["refAttribute"]:
                                            valueOfAttribute = value_list[i]
                                            break
                                    print(valueOfAttribute)
                                    fkey = "foreignKey:" + used_database + ":" + auxTable + ":" + valueOfAttribute
                                    print(fkey)
                                    if len(r.keys(fkey)) > 0:
                                        serverSocket.sendto(
                                            "CAN'T DELETE ROW FROM TABLE {} - foreign key constraint".format(
                                                table_name).encode(), address)
                                        cantdelete = 1
                        if cantdelete != 1:
                            uniqueKeys = data["databases"][used_database]["tables"][table_name]["uniqueKeys"]
                            for pos in range(len(attribute_list)):
                                for unK in uniqueKeys:
                                    if attribute_list[pos] == unK["attributeName"]:
                                        f = open(
                                            "databases/" + used_database + "/tables/" + table_name + "/" + "uniqueKey" + ".kv",
                                            "a+")
                                        f.seek(0)
                                        lines = f.readlines()
                                        final = ''
                                        for l in range(len(lines)):
                                            if value_list[pos] != lines[l].split(' ')[-1][:-1] and lines[l].split(' ')[
                                                0] == "key:":
                                                final += lines[l]
                                                final += lines[l + 1]
                                        f.close()
                                        f = open(
                                            "databases/" + used_database + "/tables/" + table_name + "/" + "uniqueKey" + ".kv",
                                            "w")
                                        f.write(final)
                                        f.close()
                            foreignKeys = data["databases"][used_database]["tables"][table_name]["foreignKeys"]
                            print(foreignKeys)
                            for td in table_data:
                                ok = 1
                                td_values = td.decode().split(":")[2] + "#" + r.get(td).decode()
                                td_values = td_values.split("#")
                                for i in range(len(value_list)):
                                    if value_list[i] != td_values[value_index[i]]:
                                        ok = 0
                                if ok == 1:
                                    r.delete(td.decode())
                                    print(len(foreignKeys))
                                    if len(foreignKeys) > 0:
                                        f_key = r.keys("foreignKey:" + used_database + ":" + table_name + ":*")
                                        if len(f_key) > 0:
                                            val = r.get(f_key[0].decode()).decode().split("#")
                                            val.remove(td.decode().split(":")[-1])
                                            newval = ""
                                            for v in val:
                                                newval += v + "#"
                                            newval = newval[:-1]
                                            if newval != "":
                                                r.set(f_key[0].decode(), newval)
                                                load_index(f_key[0].decode(), "databases/" + used_database + "/tables/" + table_name + "/foreignKey.kv")
                                            else:
                                                os.remove("databases/" + used_database + "/tables/" + table_name + "/foreignKey.kv")
                                                r.delete(f_key[0].decode())
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
    if "from" not in " ".join(statement).lower():
        serverSocket.sendto("INVALID SELECT COMMAND".encode(), address)
    else:
        if used_database:
            search_table = " ".join(statement)
            table_s = search_table[search_table.index("FROM")+5:].split(" ")[0]
            table = data["databases"][used_database]["tables"].get(table_s, None)
            if table:
                st_copy = " ".join(statement)
                if "group by" in st_copy.lower():
                    grouped = st_copy[st_copy.index("GROUP BY")+len("GROUP BY "):]
                    columns = st_copy[:st_copy.index("FROM") - 1]
                    columns = columns.split(",")
                    columns = "".join(columns).split(" ")
                    t = PrettyTable(columns)
                    ok = 0
                    index = data["databases"][used_database]["tables"][table_s]["indexFiles"]
                    indexName = ""
                    for ind in index:
                        iN = ind['indexName']
                        for i in ind['indexAttributes']:
                            if grouped in i["attributeName"]:
                                ok = 1
                                indexName = iN
                    if ok == 1:
                        l = []
                        for i in range(len(columns)):
                            l.append(" ")
                        i_gr = columns.index(grouped)
                        iKeys = r.keys(indexName + ":" + used_database + ":" + table_s + "*")
                        for k in iKeys:
                            l[i_gr] = k.decode().split(":")[-1]
                            counter = 0
                            for c in columns:
                                if counter == i_gr:
                                    counter += 1
                                if "count(" in c.lower():
                                    l[counter] = len(r.get(k).decode().split("#"))
                                if "sum(" in c.lower():
                                    suma = 0
                                    cName = c[c.index("(")+1:c.index(")")]
                                    coun = -1
                                    for s in data["databases"][used_database]["tables"][table_s]["structure"]:
                                        if s["attributeName"] == cName:
                                            break
                                        coun += 1
                                    objects = r.get(k).decode().split("#")
                                    print(objects)
                                    for ob in objects:
                                        key = used_database + ":" + table_s + ":" + ob
                                        suma += float(r.get(key).decode().split("#")[2])
                                    l[counter] = round(suma, 2)
                                if "avg(" in c.lower():
                                    suma = 0
                                    cName = c[c.index("(")+1:c.index(")")]
                                    coun = -1
                                    for s in data["databases"][used_database]["tables"][table_s]["structure"]:
                                        if s["attributeName"] == cName:
                                            break
                                        coun += 1
                                    objects = r.get(k).decode().split("#")
                                    for ob in objects:
                                        key = used_database + ":" + table_s + ":" + ob
                                        suma += float(r.get(key).decode().split("#")[2])
                                    l[counter] = round(round(suma, 2)/len(objects), 2)
                                if "min(" in c.lower():
                                    cName = c[c.index("(")+1:c.index(")")]
                                    coun = -1
                                    for s in data["databases"][used_database]["tables"][table_s]["structure"]:
                                        if s["attributeName"] == cName:
                                            break
                                        coun += 1
                                    objects = r.get(k).decode().split("#")
                                    values = []
                                    for ob in objects:
                                        key = used_database + ":" + table_s + ":" + ob
                                        values.append(float(r.get(key).decode().split("#")[2]))
                                    l[counter] = min(values)
                                if "max(" in c.lower():
                                    cName = c[c.index("(") + 1:c.index(")")]
                                    coun = -1
                                    for s in data["databases"][used_database]["tables"][table_s]["structure"]:
                                        if s["attributeName"] == cName:
                                            break
                                        coun += 1
                                    objects = r.get(k).decode().split("#")
                                    values = []
                                    for ob in objects:
                                        key = used_database + ":" + table_s + ":" + ob
                                        values.append(float(r.get(key).decode().split("#")[2]))
                                    l[counter] = max(values)
                                counter += 1
                            t.add_row(l)
                        sFile = open("databases/select.txt", 'w')
                        sFile.write(t.get_string())
                        sFile.close()
                        serverSocket.sendto("SELECT".encode(), address)
                else:
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
                        sFile = open("databases/select.txt", 'w')
                        sFile.write(t.get_string())
                        sFile.close()
                        serverSocket.sendto("SELECT".encode(), address)
                    if len(statement) > 3:
                        attributes = []
                        selection = []
                        select_type = statement[0].lower()
                        table = statement[2]
                        for str in data["databases"][used_database]["tables"][table]["structure"]:
                            attributes.append(str["attributeName"])
                        t = PrettyTable(attributes)
                        statement = statement[4:]
                        if "and" in statement:
                            statement.pop(1)
                        index = data["databases"][used_database]["tables"][table]["indexFiles"]
                        arg_attributes = []
                        arg_values = []
                        indexes = []
                        for s in statement:
                            arg_attributes.append(s.split("=")[0])
                            arg_values.append(s.split("=")[1])
                        for inx in index:
                            l = []
                            for ind in inx['indexAttributes']:
                                l.append(ind["attributeName"])
                            indexes.append(l)
                        for i in indexes:
                            if set(i).issubset(set(arg_attributes)):
                                statement_values = ""
                                for at in i:
                                    x = arg_attributes.index(at)
                                    statement_values += arg_values[x] + ":"
                                statement_values = statement_values[:-1]
                                i_key = r.keys("*:" + used_database + ":" + table + ":" + statement_values)[0].decode()
                                i_values = r.get(i_key).decode().split("#")
                                for val in i_values:
                                    l = []
                                    obj = r.get(used_database + ":" + table + ":" + val).decode().split("#")
                                    l.append(val)
                                    for o in obj:
                                        l.append(o)
                                    selection.append(l)
                                arg_attributes = list(set(arg_attributes) - set(i))
                        print(arg_attributes)
                        if len(arg_attributes) > 0:
                            print(selection)
                            if selection == []:
                                keys = r.keys(used_database + ":" + table + ":*")
                                for k in keys:
                                    l = []
                                    l.append(k.decode().split(":")[-1])
                                    obj = r.get(k.decode()).decode().split("#")
                                    for o in obj:
                                        l.append(o)
                                    selection.append(l)
                            for arg in arg_attributes:
                                ind = attributes.index(arg)
                                val = arg_values[arg_attributes.index(arg)]
                                selection = filter(lambda item: item[ind] == val, selection)
                        copy = []
                        if select_type == "distinct":
                            for s in selection:
                                if r.get(used_database + ":" + table + ":" + s[0]).decode() not in copy:
                                    copy.append(r.get(used_database + ":" + table + ":" + s[0]).decode())
                                    t.add_row(s)
                        else:
                            for s in selection:
                                t.add_row(s)
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
    destinatie = ['SUA', "Canada", 'Japonia', 'Mexic', "Egipt", 'Indonezia', 'Columbia']
    for i in range(10000):
        com = random.choice(companie)
        plec = random.choice(plecare)
        dest = random.choice(destinatie)
        k = str(i + 1)
        key = "d2:Zboruri:" + k
        value = com + '#' + plec + '#' + dest
        r.set(key, value)
        skey1 = "i1Index:d2:Zboruri:" + plec + ':' + dest
        skey2 = "i2Index:d2:Zboruri:" + com

        search_ikey = r.keys(skey1)
        if len(search_ikey) > 0:
            ivalue = r.get(skey1).decode()
            ivalue += "#" + k
            r.set(skey1, ivalue)
        else:
            r.set(skey1, k)

        search_ikey = r.keys(skey2)
        if len(search_ikey) > 0:
            ivalue = r.get(skey2).decode()
            ivalue += "#" + k
            r.set(skey2, ivalue)
        else:
            r.set(skey2, k)
    load_index("i1Index:d2:Zboruri:*", "databases/d2/tables/Zboruri/i1Index.kv")
    load_index("i2Index:d2:Zboruri:*", "databases/d2/tables/Zboruri/i2Index.kv")
    serverSocket.sendto("Generated 1 million values for Zboruri".encode(), address)


def get_random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for _ in range(length))


def generate2():
    country = ['Spania', 'Anglia', 'Germania', 'Franta', 'Olanda', 'Portugalia', 'Italia', 'SUA', "Canada", 'Japonia', 'Mexic', "Egipt", 'Indonezia', 'Columbia']
    for i in range(1000):
        coun = random.choice(country)
        name = get_random_string(16)
        wage = round(random.uniform(1000, 85000), 2)
        k = str(i + 1)
        key = "d2:Employee:" + k
        value = name + '#' + coun + '#' + str(wage)
        r.set(key, value)
        skey1 = "ind1Index:d2:Employee:" + coun

        search_ikey = r.keys(skey1)
        if len(search_ikey) > 0:
            ivalue = r.get(skey1).decode()
            ivalue += "#" + k
            r.set(skey1, ivalue)
        else:
            r.set(skey1, k)
    load_index("ind1Index:d2:Employee:*", "databases/d2/tables/Employee/ind1Index.kv")
    serverSocket.sendto("Generated 1 million values for Employee".encode(), address)


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
    elif clientData[0].lower() == "generate2":
        generate2()
    else:
        serverSocket.sendto("INVALID COMMAND".encode(), address)
