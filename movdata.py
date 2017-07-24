import MySQLdb
import os

def movData(tetrisDB, flist, exp_id):
	pn = 'p'+ str(exp_id)
	eid = exp_id
	with open(flist,'r') as fl:
		for line in fl:
			#catch each session 
			sesn = line
			try:
				cursor = tetrisDB.cursor()
				# header partition_num exp_id session
				# header is original table's content
				# leave new table's field to be null
				queryinsert = """insert into completeTable ({}) select * from complete partition({}) where exp_id = {} and session = '{}' """
				querydelete = """delete from complete partition({}) where exp_id = {} and session = '{}' """
				cursor.execute(queryinsert.format(header, pn, eid, sesn))
				cursor.execute(querydelete.format(pn, eid, sesn))
				cursor.commit()
				print ("finish transfer session "+ sesn + " in experiment " + str(eid))
			except:
				tetrisDB.rollback()

# test for single row of data move
# not used for new 
def mov(tetrisDB):
	try:
		cursor = tetrisDB.cursor()
		iquery = """insert into moveCheck (id, name) select * from testTB where id = {}"""
		dquery = """delete from testTB where id = {}"""
		cursor.execute(iquery.format(4))
		cursor.execute(dquery.format(4))
		tetrisDB.commit()
	except:
		tetrisDB.rollback()


def extsession(expfolder,year):
	# iterate through files and search for session 
	record = open("record.txt",'w')
	for root, dirs, files in os.walk(expfolder):
		for data in files:
			if "games" not in data and "eps" not in data and "incomplete" not in data and "episodes" not in data and ".tsv" in data:
				inbegin = data.find(year)
				inend = data.find(".tsv")
				if (inbegin != inend and year in data[inbegin:inend]):
					record.write(data[inbegin:inend])
					record.write("\n")

if __name__ == "__main__":
	tetrisDB = MySQLdb.connect(host = "localhost", user = "root", passwd = "", db = "testDB")
	#movData(tetrisDB, flist , exp_id)
	#mov(tetrisDB)
	extsession("/Volumes/research/CogWorksLab/cogback/Projects/Tetris/Lab_Experiments/2015_Tetris_Tutor/data/D1","2015")
	tetrisDB.close()