import MySQLdb
import os
import csv
from datetime import datetime

"""
Error :
Traceback (most recent call last):
  File "loadfile.py", line 1, in <module>
    import MySQLdb
  File "/usr/local/lib/python2.7/site-packages/MySQLdb/__init__.py", line 19, in <module>
    import _mysql
ImportError: dlopen(/usr/local/lib/python2.7/site-packages/_mysql.so, 2): Library not loaded: libmysqlclient.18.dylib
  Referenced from: /usr/local/lib/python2.7/site-packages/_mysql.so
  Reason: image not found

Use the following command in terminal will solve the problem
export DYLD_LIBRARY_PATH=/usr/local/mysql/lib/
"""


def prep(inFileName, exp_id, outFileName, crpFile): # used for converting data collected after 2013
    if inFileName == "" and outFileName == "": # no fileName is provided
        return
    with open(inFileName,'rb') as inData, open(outFileName, 'wb') as outData:
        inData = csv.reader(inData,delimiter='\t')
        outData = csv.writer(outData, delimiter = "\t")
        hdFlag = 0 # if it is a header line
        hdCount = 0 # new field, such as 'agree' appeared at some time in previous file
        for row in inData:
            if hdFlag == 0 and 'exp_id' not in row:#insert exp_id header
                hdCount = len(row)
                row.append('exp_id')
                hdFlag = 1
            else:
                if len(row) < hdCount:
                    num = hdCount - len(row)
                    for i in range(num):
                        row.append("\N")
                elif len(row) > hdCount:
                    crpFile.write(inFileName+"\n ... \n"+row+"\n")
                    continue
                row.append(str(exp_id))
            for i,value in enumerate(row): # clean up all the empty '' to '\N' for easier inserting
                if value == "" or value == "None": # allow SQL recognize NULL
                    row[i] = "\N"
            outData.writerow(row) # record which data has been inserted


def prep13(inFileName, outFileName, exp_id, crpFile): #used for convert 2013 data
    if inFileName == "" or outFileName == "":
        return
    else:
        with open(inFileName, 'rb') as inData, open(outFileName, 'wb') as outData:
            inData = csv.reader(inData, delimiter = "\t")
            outData = csv.writer(outData, delimiter = "\t")
            '''the order of the inserting format for 2013 output data'''
            attributes = ["ts", "event_type", "session", "game_number", "episode_number", "level", "score", "lines_cleared", "completed", "zoid_sequence", "evt_id", "evt_data1", "evt_data2", "curr_zoid", "next_zoid", "danger_mode","delaying", "dropping", "zoid_rot", "zoid_col", "zoid_row", "board_rep", "zoid_rep", "smi_ts", "smi_eyes", "smi_samp_x_l", "smi_samp_x_r", "smi_samp_y_l", "smi_samp_y_r", "smi_diam_x_l", "smi_diam_x_r", "smi_diam_y_l", "smi_diam_y_r", "smi_eye_x_l", "smi_eye_x_r", "smi_eye_y_l", "smi_eye_y_r", "smi_eye_z_l", "smi_eye_z_r", "fix_x", "fix_y","pits", "tetris_progress", 'game_seed', 'height', 'avgheight', 'roughness', 'ridge_len', 'ridge_len_sqr', 'tetris_available', 'filled_rows_covered', 'tetrises_covered', 'good_pos_curr', 'good_pos_next', 'good_pos_any',"exp_id"]
            evt = 0 #the unknown column is evt data column or not
            outData.writerow(attributes)
            for row in inData:
                err = 0
                wrow = ["\N"]*len(attributes)
                for i, value in enumerate(row):
                    index = -1
                    if value != "":
                        if value[0] == ':':
                            if value[1:] in attributes:
                                index = attributes.index(value[1:])
                                wrow[index] = row[i+1]
                            else:
                                if evt == 0: #the unknown : is belong evt_id
                                    wrow[attributes.index("evt_id")] = value[1:]
                                    wrow[attributes.index("evt_data1")] = row[i+1]
                                    wrow[attributes.index("evt_data2")] = "setup"
                                else: #write to crpFile
                                    crpFile.write(str(row)+"\n")
                                    err = 1
                        else:
                            continue
                wrow[-1] = exp_id #write exp_id to each line of output data
                if err == 0:
                    outData.writerow(wrow)
                if "Start" in wrow:
                    evt = 1


def itrDir(inDir, exp_id, outDir, crpFile, thir): #temporarily put files in the folder on the desktop
    for root, dirs, files in os.walk(inDir):
        for name in files:
            if thir == "1":
                if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name and "complete_" in name and "2013" not in name and "lock" not in name:
                    outFileName = ''.join([outDir, name])
                    inFileName = os.path.join(root,name)
                    prep(inFileName, exp_id, outFileName, crpFile)
                else:
                    continue
            elif thir == "2":
                if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name and ".incomplete" not in name and "2013" in name and "lock" not in name:
                    outFileName = ''.join([outDir, name])
                    inFileName = os.path.join(root,name)
                    prep13(inFileName, outFileName,exp_id,crpFile)
                else:
                    continue

def loadFile(directory,tetrisDB,ef,imf):
    for subdir, dirs, files in os.walk(directory):
        for data in files:
            if "games" not in data and "eps" not in data and "incomplete" not in data and "episodes" not in data and ".tsv" in data:
                dataDir = os.path.join(subdir, data)
                dataDir = os.path.abspath(dataDir)
                header = ""
                #print ("V loading, ", dataDir)
                with open(dataDir,'rb') as inData:
                    inData = csv.reader(inData,delimiter='\t')
                    for row in inData:
                        header = ','.join(row)
                        break
                try: # load into database
                    cursor = tetrisDB.cursor()
                    query = """LOAD DATA LOCAL INFILE '{}' INTO TABLE completeTbl FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ({}); """
                    cursor.execute(query.format(dataDir, header))
                    tetrisDB.commit()
                    imf.write(dataDir+" \n")
                except: # fail then rollback
                    ef.write("Exception: "+dataDir+" \n")
                    tetrisDB.rollback()


if __name__ == "__main__":
    convIm = raw_input("1. convert 2.import -> ")
    if convIm == "1":
        inDir = raw_input("give the experiment folder name -> ") #enter with '/' in the path
        outDir = raw_input("give the output folder name -> ")
        exp_id = raw_input("assign the experiment id -> ")
        data13 = raw_input("1.after 2013 2.2013 -> ")
        crpFileName = "corrupted-"+str(datetime.now())+".txt"
        crpFile = open(crpFileName,"w")
        itrDir(inDir,exp_id,outDir,crpFile,data13)
        crpFile.close()
        conv = raw_input("1. Import 2. Do not import -> ")
        if conv == "1":
            tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB", local_infile = 1)
            errFileName = "err-"+str(datetime.now())+".txt"
            impFileName = "ipt-"+str(datetime.now())+".txt"
            errFile = open(errFileName, "w")
            impFile = open(impFileName,"w")
            loadFile(outDir,tetrisDB, errFile, impFile)
            errFile.close()
            impFile.close()
            tetrisDB.close()
    elif convIm == "2":
        outDir = raw_input("give the output folder name -> ")
        exp_id = raw_input("confirm the experiment id -> ")
        tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB", local_infile = 1)
        errFileName = "err-"+str(datetime.now())+".txt"
        impFileName = "ipt-"+str(datetime.now())+".txt"
        errFile = open(errFileName, "w")
        impFile = open(impFileName,"w")
        loadFile(outDir,tetrisDB, errFile, impFile)
        errFile.close()
        impFile.close()
        tetrisDB.close()
