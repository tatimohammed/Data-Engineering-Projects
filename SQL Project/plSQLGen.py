import pandas as pd
from datetime import datetime


class PlSQL_Generator:
    def __init__(self) -> None:
        pass

    def generate_ids(self, csv_file, y=0, z=0):
            data = pd.read_csv(csv_file)
            id_ = []
            for line in range(data.shape[0]):
                if str(data.SEMESTRE[line]) == 'nan':
                    x = data.AN[line]
                    id_mod_ele = data.SCHOOL[0][-1]+'I'+data.FIELD[0]+str(x)+'04'
                    id_.append(id_mod_ele)
                elif str(data.SEMESTRE[line]) != 'nan' and str(data.MODULE[line]) == 'nan':
                    x = data.SEMESTRE[line][1]
                    id_mod_ele = data.SCHOOL[0][-1]+'I'+data.FIELD[0]+str(x)+'004'
                    id_.append(id_mod_ele)
                    y = 0
                else:
                    x = data.SEMESTRE[line][1]
                    if line > 1:
                        if x == data.SEMESTRE[line-1][1]:
                            if str(data.ELEMENT[line]) == 'nan':
                                y += 1
                                z = 0
                            else:
                                z += 1
                        else:
                            y = 0
                            if str(data.ELEMENT[line]) == 'nan':
                                y += 1
                                z = 0
                            else:
                                z += 1
                        id_mod_ele = data.SCHOOL[0][-1]+'I'+data.FIELD[0]+str(x)+str(y)+str(z)+'4'
                        id_.append(id_mod_ele)
                    else:
                        y = 1
                        z = 0
                        id_mod_ele = data.SCHOOL[0][-1]+'I'+data.FIELD[0]+str(x)+str(y)+str(z)+'4'
                        id_.append(id_mod_ele)
            return pd.DataFrame({"ID": id_})

    def final_data(self, csv_file):
        data = pd.read_csv(csv_file)
        ids = self.generate_ids(csv_file)
        data2 = pd.concat([ids, data], axis=1)
        data2['IS'] = None
        data2.loc[data2.ELEMENT.isnull(), 'IS'] = 'MOD'
        data2.loc[~data2.ELEMENT.isnull(), 'IS'] = 'ELM'
        data2.loc[data2.MODULE.isnull(), 'IS'] = 'SEM'
        data2.loc[data2.SEMESTRE.isnull(), 'IS'] = 'AN'
        data2.fillna('_', inplace=True)
        return data2

    def generatePlSQL(self, csv_file):
        count = 0
        data2 = self.final_data(csv_file)
        m = data2.shape[0]
        with open("log.txt", 'a') as log:
            log.write("---\n")
            log.write(f"The excution of the script for: {data2.SCHOOL[0]}\n")
            log.write("---\n")
            log.write("-----------------------------------------\n")
            log.write("The File generation started successfully!\n")
            log.write("-----------------------------------------\n")
            log.write("Start Time: \n")
            log.write(f"\t{str(datetime.now())}\n")
            log.write("-----------------------------------------\n")
            date = str(datetime.now())
            with open(str(data2.SCHOOL[0])+" - "+str(data2.FIELD[0])+" - "+date[:date.index('.')]+".sql", 'w') as f:
                for line in range(m):
                    if data2.SEMESTRE[line] == '_':
                        comm_data = "-- "+str(data2.AN[line])+"ère Année:\n"
                        f.write(comm_data)
                        line_data = "insert into Mod_Elem_id VALUES ('" + \
                            data2.ID[line] + \
                            "','EN"+data2.SCHOOL[0][-1]+"','AN','','VET_ELEM_NOM','VET_ELEM_NOM');\n"
                        f.write(line_data)
                    elif data2.SEMESTRE[line] != '_' and data2.MODULE[line] == '_':
                        comm_data = "-- "+data2.SEMESTRE[line]+"\n"
                        f.write(comm_data)
                        line_data = "insert into Mod_Elem_id VALUES ('"+data2.ID[line]+"','EN"+data2.SCHOOL[0][-1]+"','SM0"+data2.SEMESTRE[line][1]+"','" + \
                            data2.SEMESTRE[line]+"','semestre "+data2.SEMESTRE[line][1] + \
                            "','semestre "+data2.SEMESTRE[line][1]+"');\n"
                        f.write(line_data)
                    else:
                        line_data = "insert into Mod_Elem_id VALUES ('"+data2.ID[line]+"','EN"+data2.SCHOOL[0][-1]+"','"+data2.IS[line] + \
                            "', '" + \
                            data2.SEMESTRE[line]+"', '"+data2.MODULE[line] + \
                            "', '"+data2.ELEMENT[line]+"' );\n"
                        f.write(line_data)
                    count += 1
                f.write("COMMIT;")
                for line in range(m):
                    if len(data2.ID[line]) == 7:
                        f.write("\n\n")
                    data_line = "insert into Inscr_Pedag_Id values ('EN"+data2.SCHOOL[0][-1]+"','" + \
                        data2.ID[line]+"');\n"
                    f.write(data_line)
                    count += 1
                f.write("COMMIT;\n")
            f.close()
            log.write("The File has been Generated successful!\n")
            log.write("-----------------------------------------\n")
            log.write("End Time: \n")
            log.write(f"\t{str(datetime.now())}\n")
            log.write(f"\t{count} lines have been inserted\n")
            log.write("-----------------------------------------\n")
            log.write("--------- Generated by TweetyX ----------\n")
            log.write("\n\n")
        log.close()
