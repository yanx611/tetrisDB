import csv


def prep(filename,store):
	with open(filename,'rb') as indata, open(store, 'wb') as outdata:
		indata = csv.reader(indata,delimiter='\t')
		outdata = csv.writer(outdata, delimiter = "\t")
		headerflag = 0
		for row in indata:
			if (headerflag == 0):
				row.append('exp_id')
				headerflag = 1
			else:
				row.append(1)
			for i,value in enumerate(row):
				if (value == ""):
					row[i] = "\N"
			outdata.writerow(row)	


def main():
	fn = raw_input("give the file name: ")
	ot = raw_input("give the output dir:")
	prep(fn,ot)


if __name__ == "__main__":
	main()




