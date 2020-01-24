#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import glob
import csv
import os


# export options
csv_tf = True #.csv, default
csv_sep = '\t'
db_tf = True #.sl3 sqlite3 db
pickle_tf = True #.pkl pickle of pandas dataframe
#hdf5_tf = True #.h5, not working
json_tf = True #.json
# file name and path
prefix = 'output'+os.sep+'neiss'


def main(file_list=[]):

	if len(file_list)==0:
		file_list = glob.glob('data'+os.sep+'neiss*.tsv')

	cols = ['case_id','trmt_date','psu','weight','stratum','age','sex','race','race_other','diag','diag_other','body_part','disposition','location','fmv','prod1','prod2','narr1','narr2']
	
	for f in file_list:
		print (f)
		# create temp dataframe per file
		# full options - http://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
		tt = pd.read_csv(f, sep='\t', names=cols, header=1, quoting=csv.QUOTE_NONE, low_memory=False, \
							skipinitialspace=True, warn_bad_lines=True, error_bad_lines=False, \
							parse_dates=False, \
							 encoding='utf-8', \
							) #nrows =10000)
		
		# add sourcefile to data
		tt['sourcefile'] = f
		tt['trmt_date'] = pd.to_datetime(tt.trmt_date, format='%m/%d/%Y').dt.date

		# create real dataframe, or append to it if it exists already
		if 'df' in locals() or 'df' in globals():
			df = pd.concat([df,tt], axis=0)
		else:
			df = tt

		# set index after adding new file
		df.set_index('case_id')

		print (tt.shape[0],'records from',f)
	# exit loop over raw data

	# clean data in one data frame
	print (df.shape[0],'records in combined output')

	# decoding sex
	conditions = [ (df.sex == 1), (df.sex == 2), (df.sex == 0)] 
	choice = ['Male', 'Female', 'Not recorded']
	df['sex'] = np.select(conditions, choice, default=df.sex)

	# decoding age
	df['age'] = pd.to_numeric(df['age'], errors='coerce')
	df['age'] = np.where(df.age==0, np.NaN , df.age)
	df['age'] = np.where(df.age>199, ((df.age - 200)/12), df.age)

	# decoding race
	conditions = [ (df.race == 1), (df.race == 2), (df.race == 3), (df.race == 4), (df.race == 5), (df.race == 6), (df.race == 0)  ] 
	choice = ['White', 'Black/African American', 'Asian', 'American Indian/Alaska Native', 'Native Hawaiian/Pacific Islander', 'Other', 'Not Stated in ED record']
	df['race_text'] = np.select(conditions, choice, default=df.race)

	# decoding location
	conditions = [ (df.location == 1), (df.location == 2), (df.location == 4), (df.location == 5), (df.location == 6), (df.location == 7), (df.location == 8), (df.location == 9), (df.location == 0) ] 
	choice = ['Home', 'Farm/ranch', 'Street or highway', 'Other public property', 'Mobile/Manufactured home', 'Industrial', 'School/Daycare', 'Place of recreation or sports','Not recorded']
	df['location'] = np.select(conditions, choice, default=df.location)

	# decoding fire involvement
	conditions = [ (df.fmv == 1), (df.fmv == 2), (df.fmv == 3), (df.fmv == 0)] 
	choice = ['Fire involvement and/or smoke inhalation - Fire Dept. attended', \
				'Fire involvement and/or smoke inhalation - Fire Dept. did not attend', \
				'Fire involvement and/or smoke inhalation - Fire Dept. attendance is not recorded', \
				'No fire involvement or fire involvement not recorded']
	df['fire_involvement'] = np.select(conditions, choice, default=df.fmv)
	df.drop('fmv', axis=1, inplace=True)

	# decoding hospital strata
	conditions = [ (df.stratum == 'C'), (df.stratum == 'V'), (df.stratum == 'L'), (df.stratum == 'M'), (df.stratum == 'S') ] 
	choice = ['Children\'s Hospitals', 'Very Large Hospitals', 'Large Hospitals', 'Medium Hospitals', 'Small Hospitals']
	df['hospital_stratum'] = np.select(conditions, choice, default=df.stratum)
	df.drop('stratum', axis=1, inplace=True)

	# renaming hospital id
	df.rename(columns={'psu': 'patient_service_unit'}, inplace=True)
	df.rename(columns={'weight': 'psu_weight'}, inplace=True)

	# decoding disposition
	conditions = [ (df.disposition == 1), (df.disposition == 2), (df.disposition == 4), (df.disposition == 5), (df.disposition == 6), (df.disposition == 8), (df.disposition == 9)] 
	choice = ['Treated and released or examined and released without treatment', \
				'Treated and transferred to another hospital', \
				'Treated and admitted for hospitalization (within same facility)', \
				'Held for observation', \
				'Left without being seen/Left against medical advice (AMA)', \
				'Fatality, including DOA, died in the ED, brain dead', \
				'Not recorded']
	df['disposition'] = np.select(conditions, choice, default=df.disposition)

	# decoding diagnosis
	diag_dict = {50 : u'Amputation'
				,65 : u'Anoxia'
				,42 : u'Aspirated foreign object'
				,72 : u'Avulsion'
				,48 : u'Burns, scald (from hot liquids or steam)'
				,51 : u'Burns, thermal (from flames or hot surface)'
				,49 : u'Burns, chemical (caustics, etc.)'
				,73 : u'Burns, radiation (includes all cell damage by ultraviolet, x-rays, microwaves, laser beam, radioactive materials, etc.)'
				,46 : u'Burns, electrical'
				,47 : u'Burns, not specified'
				,52 : u'Concussions'
				,53 : u'Contusions, Abrasions'
				,54 : u'Crushing'
				,60 : u'Dental injury'
				,74 : u'Dermatitis, Conjunctivitis'
				,55 : u'Dislocation'
				,67 : u'Electric shock'
				,56 : u'Foreign body'
				,57 : u'Fracture'
				,58 : u'Hematoma'
				,66 : u'Hemorrhage'
				,41 : u'Ingested foreign object'
				,62 : u'Internal organ injury'
				,59 : u'Laceration'
				,61 : u'Nerve damage'
				,68 : u'Poisoning'
				,63 : u'Puncture'
				,64 : u'Strain or Sprain'
				,69 : u'Submersion (including Drowning)'
				,71 : u'Other/Not Stated'}

	df['diag'] = df.diag.replace(diag_dict)

	# decoding body part
	body_dict = {33 : u'Arm, lower (not including elbow or wrist)'
				,80 : u'Arm, upper'
				,37 : u'Ankle'
				,94 : u'Ear'
				,32 : u'Elbow'
				,77 : u'Eyeball'
				,76 : u'Face (including eyelid, eye area and nose)'
				,92 : u'Finger'
				,83 : u'Foot'
				,82 : u'Hand'
				,75 : u'Head'
				,0 : u'Internal (use with aspiration and ingestion)'
				,35 : u'Knee'
				,36 : u'Leg, lower (not including knee or ankle)'
				,81 : u'Leg, upper'
				,88 : u'Mouth (including lips, tongue and teeth)'
				,89 : u'Neck'
				,38 : u'Pubic region'
				,30 : u'Shoulder (including clavicle, collarbone)'
				,93 : u'Toe'
				,79 : u'Trunk, lower'
				,31 : u'Trunk, upper (not including shoulders)'
				,34 : u'Wrist'
				,84 : u'25-50% of body'
				,85 : u'All parts of body (more than 50% of body)'
				,87 : u'Not recorded'}

	df['body_part'] = df.body_part.replace(body_dict)

	# decoding product codes, with merge
	pc = parse_product_codes()
	try:
		df['prod1_text'] = df.prod1.astype(float).replace(pc)
	except:
		pass
	try:
		df['prod2_text'] = df.prod2.astype(float).replace(pc)
	except:
		pass

	# combine narrative fields, if they exist
	try:
		df['narr'] = df.narr1 + ' ' + df.narr2
		df.drop('narr1', axis=1, inplace=True)
		df.drop('narr2', axis=1, inplace=True)
	except:
		pass # old files don't have narrative

	# write data to sqlite3 database
	if db_tf:
		import sqlite3
		cnx = sqlite3.connect(prefix+'.sl3')
		df.to_sql(con=cnx, name=prefix+'_t', if_exists='replace', index=True)

	if csv_tf:
		df.to_csv(prefix+'.csv',sep=csv_sep)

	if json_tf:
		d = df.to_json(prefix+'.json', orient='records', date_format='iso', date_unit='s')

		if False: # testing
			import json
			with open(prefix+'.json','r') as i:
				j = json.load(i)
				print (json.dumps(j[-1],indent=4))

	# http://stackoverflow.com/a/17098736/2327328
	if pickle_tf:
		df.to_pickle(prefix+'.pkl') 
		# to read
		#df = pd.read_pickle(file_name)

	# not working
	#if hdf5_tf:
	#	import HDFStore
	#	store = HDFStore(prefix+'.h5')
	#	store['df'] = df
	#	# load it
	#	# store['df']  # load it

def parse_product_codes():
	with open('metadata'+os.sep+'product_codes.tsv','r') as p:
		pc = pd.read_csv(p,sep='\t')
		pc['product_code'] = pc.product_code.map(float)
		return pc.set_index('product_code').T.to_dict(orient='index').get('product_text')

if __name__ == "__main__":
	#print parse_product_codes()
	#main()
	#file_list = glob.glob('raw_data\\*.tsv')
	#for f in file_list:
	main()