import MySQLdb
import os
import csv

# export DYLD_LIBRARY_PATH=/usr/local/mysql/lib/

def prep(filename, exp_id, store, ln):
	if (filename == ""):
		return
	print ("converting", filename)
	with open(filename,'rb') as indata, open(store, 'wb') as outdata:
		indata = csv.reader(indata,delimiter='\t')
		outdata = csv.writer(outdata, delimiter = "\t")
		headerflag = 0
		headercount = 0
		for row in indata:
			if (headerflag == 0):
				headercount = len(row)
				row.append('exp_id')
				headerflag = 1
			else:
				if (len(row) != headercount):
					num = headercount - len(row)
					for i in range(num):
						row.append("\N")
				elif (len(row) > headercount):
					#record the data and skip
					ln.write(filename)
					ln.write("\n ... \n")
					ln.write(row)
					ln.write("\n")		
					continue
				row.append(str(exp_id))
			for i,value in enumerate(row):
				if (value == "" or value == "None"):
					row[i] = "\N"
			outdata.writerow(row)

def singConv(infile, outfile, exp_id, error):
	if (infile == "" or outfile == ""):
		return
	else:
		with open(infile, 'rb') as indata, open(outfile, 'wb') as outdata:
			indata = csv.reader(indata, delimiter = "\t")
			outdata = csv.writer(outdata, delimiter = "\t")
			attributes = ["ts", "event_type", "session", "game_number", "episode_number", "level", "score", "lines_cleared", "completed", "zoid_sequence", "evt_id", "evt_data1", "evt_data2", "curr_zoid", "next_zoid", "danger_mode","delaying", "dropping", "zoid_rot", "zoid_col", "zoid_row", "board_rep", "zoid_rep", "smi_ts", "smi_eyes", "smi_samp_x_l", "smi_samp_x_r", "smi_samp_y_l", "smi_samp_y_r", "smi_diam_x_l", "smi_diam_x_r", "smi_diam_y_l", "smi_diam_y_r", "smi_eye_x_l", "smi_eye_x_r", "smi_eye_y_l", "smi_eye_y_r", "smi_eye_z_l", "smi_eye_z_r", "fix_x", "fix_y","pits", "tetris_progress", 'game_seed', 'height', 'avgheight', 'roughness', 'ridge_len', 'ridge_len_sqr', 'tetris_available', 'filled_rows_covered', 'tetrises_covered', 'good_pos_curr', 'good_pos_next', 'good_pos_any',"exp_id"]
			# if the unknown column is evt column
			evt = 0
			outdata.writerow(attributes)
			for row in indata:
				err = 0
				wrow = ["\N"]*len(attributes)
				for i, value in enumerate(row):
					index = -1
					if (value != ""):
						if (value[0] == ':'):
							if (value[1:] in attributes):
								index = attributes.index(value[1:])
								wrow[index] = row[i+1]
							else:
								if (evt == 0):
									# then the unknown : is belong evt_id
									wrow[attributes.index("evt_id")] = value[1:]
									wrow[attributes.index("evt_data1")] = row[i+1]
									wrow[attributes.index("evt_data2")] = "setup"
								else:
									#write to error
									error.write(str(row))
									error.write('\n')
									err = 1
						else:
							continue
				wrow[-1] = exp_id
				if (err == 0):
					outdata.writerow(wrow)
				if ("Start" in wrow):
					evt = 1


def itrDir(exp_folder, exp_id, storing_path, ln, thir):
	#temporarily put files in the folder on the desktop
	for root, dirs, files in os.walk(exp_folder):
		for name in files:
			if (thir == "1"):
				if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name and "complete_" in name and "2013" not in name and "lock" not in name:
					outname = ''.join([storing_path, name])
					inname = os.path.join(root,name)
					prep(inname, exp_id, outname, ln)
				else:
					continue
			elif(thir == "2"):
				if "games" not in name and "eps" not in name and "incomplete" not in name and "episodes" not in name and ".tsv" in name and ".incomplete" not in name and "2013" in name and "lock" not in name:
					outname = ''.join([storing_path, name])
					inname = os.path.join(root,name)
					singConv(inname, outname,exp_id,ln)
				else:
					continue

def loadFile(directory,tetrisDB,ef,imf):
	for subdir, dirs, files in os.walk(directory):
		for data in files:
			if "games" not in data and "eps" not in data and "incomplete" not in data and "episodes" not in data and ".tsv" in data:
				datadir = os.path.join(subdir, data)
				datadir = os.path.abspath(datadir)
				# load into database 
				# try:
				header = ""
				print ("V loading, ", datadir)
				with open(datadir,'rb') as indata:
					indata = csv.reader(indata,delimiter='\t')
					for row in indata:
						header = ','.join(row)
						break
				try:
					cursor = tetrisDB.cursor()
					query = """LOAD DATA LOCAL INFILE '{}' INTO TABLE complete FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\n' IGNORE 1 LINES ({}); """
					cursor.execute(query.format(datadir, header))
					tetrisDB.commit()
					imf.write(data)
					imf.write("\n")
				except:
					ef.write("Exception: ")
					ef.write(data)
					ef.write("\n")
					tetrisDB.rollback()


if __name__ == "__main__":
	convim = raw_input("1. convert+import 2.import -> ")
	if (convim == "1"):
		indir = raw_input("give the experiment folder name: ")
		#enter with the / in the path
		ot = raw_input("give the output folder name: ")
		exp_id = raw_input("assign the experiment id: ")
		part = raw_input("assign the part id: ")
		ln = "wrongdata-"+str(exp_id)+"-"+str(part)+".txt"
		lnfile = open(ln,"w")
		#first convert all the data
		thirt = raw_input("1.after 2013 2.2013")
		itrDir(indir,exp_id,ot,lnfile,thirt)
		lnfile.close()
		#then using the loaddata function to input all the data into the database
		#have a file to write errors
		conv = raw_input("Import or not?")
		if (conv == "Yes"):
			tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB", local_infile = 1)
			efn = "errors-"+str(exp_id)+"-"+str(part)+".txt"
			ifn = "imported"+str(exp_id)+"-"+str(part)+".txt"
			errorfile = open(efn, "w")
			importfile = open(ifn,"w")
			loadFile(ot,tetrisDB, errorfile, importfile)
			errorfile.close()
			importfile.close()
			tetrisDB.close()
	elif (convim == "2"):
		ot = raw_input("give the output folder name: ")	
		exp_id = raw_input("assign the experiment id: ")
		part = raw_input("assign the part id: ")
		tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "password123", db = "TetrisDB", local_infile = 1)
		efn = "errors-"+str(exp_id)+"-"+str(part)+".txt"
		ifn = "imported-"+str(exp_id)+"-"+str(part)+".txt"
		errorfile = open(efn, "w")
		importfile = open(ifn,"w")
		loadFile(ot,tetrisDB, errorfile, importfile)
		errorfile.close()
		importfile.close()
		tetrisDB.close()

				
	
	



