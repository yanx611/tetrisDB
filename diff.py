import csv


def diff(original, modified):
	with open(original,'rb') as ori, open(modified, 'rb') as modi:
		ori = csv.reader(ori, delimiter = '\t')
		modi = csv.reader(modi, delimiter = '\t')
		sameflag = 0;
		headrow = 0;
		count = 0;
		for orow, mrow in zip(ori, modi):
			count = count + 1
			if (headrow == 0):
				print orow
			else:
				for i, j in zip(orow, mrow):
					if (str(i)[:7] != str(j)[:7] and sameflag != 1 and headrow == 1):
						sameflag = 1
					break			
			if (sameflag == 1 ):
				print orow
				if (sameflag == 1):
					break
			headrow = 1
	print ("end check")

			

def find(ts, original):
	with open(original,'rb') as ori:
		ori = csv.reader(ori, delimiter = '\t')
		sameflag = 0;
		headrow = 0;
		for orow in ori:
			for i in orow:
				if (i[:7] == str(ts)[:7] and headrow == 1):
					sameflag == 1
					print orow
				break
			if (headrow == 0):
				print orow
			if (sameflag == 1 ):
				sameflag = 0
				print orow
			headrow = 1
	print ("end check")	

def main():
	original = raw_input("give the original file path : ")
	findts = raw_input("1. ts 2. compare:")
	if (int(findts) == 1):
		ts = raw_input("give ts: ")
		find(ts,original)
	elif(int(findts) == 2):
		modified = raw_input("give the modified file path: ")
		diff(original, modified)
	else:
		return


if __name__ == "__main__":
	main()



